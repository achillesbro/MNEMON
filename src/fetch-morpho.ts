import 'dotenv/config';
import { createPublicClient, http, parseAbi, type Address } from 'viem';
import fs from 'node:fs/promises';
import path from 'node:path';

// ---- Config & constants -----------------------------------------------------

const RPC_URL = process.env.HYPEREVM_RPC_URL!;
const VAULT = (process.env.PUBLIC_ALLOCATOR || process.env.VAULT_ADDRESS)! as Address; // USDT0 vault
const WHYPE_VAULT = '0x889d35426F44A06EE89adF1eC4E5A4C9EB50a4f1' as Address; // WHYPE vault
const MORPHO_BLUE = process.env.MORPHO_BLUE! as Address;
const MORPHO_LENS = process.env.MORPHO_LENS as Address | undefined;

const SECONDS_PER_YEAR = 31_536_000; // 365d
const WAD = 10n ** 18n;

// HyperEVM chain ID for Morpho API
const CHAIN_ID = 999;

// Known token addresses for vault selection
const USDT0_ADDRESS = '0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb' as Address;
const WHYPE_ADDRESS = '0x5555555555555555555555555555555555555555' as Address;

if (!RPC_URL || !MORPHO_BLUE || !VAULT) {
  throw new Error('Missing env: HYPEREVM_RPC_URL, MORPHO_BLUE, VAULT_ADDRESS/PUBLIC_ALLOCATOR');
}

// ---- ABIs (minimal) ---------------------------------------------------------

// Standard ERC20
const ERC20_ABI = parseAbi([
  'function decimals() view returns (uint8)',
  'function balanceOf(address) view returns (uint256)'
]);

/**
 * TODO: Paste your Morpho Blue ABI views needed for:
 * - totalSupplyAssets / totalBorrowAssets (direct or via shares+indexes)
 * - user supplyShares for (marketId, VAULT)
 *
 * If you have a Lens on HyperEVM, paste its ABI views instead and prefer Lens reads.
 */
const LENS_ABI = MORPHO_LENS
  ? parseAbi([
      // Replace with your deployed Lens interface:
      'function totalSupplyAssets(bytes32) view returns (uint256)',
      'function totalBorrowAssets(bytes32) view returns (uint256)',
      'function supplyShares(bytes32,address) view returns (uint256)'
    ])
  : undefined;

// ---- Morpho Blue core (read-only) -------------------------------------------
const MORPHO_BLUE_ABI = parseAbi([
  // Market state (totals & fee). Returns the Market struct.
  'function market(bytes32 id) view returns (uint128 totalSupplyAssets,uint128 totalSupplyShares,uint128 totalBorrowAssets,uint128 totalBorrowShares,uint128 lastUpdate,uint128 fee)',
  // Map id → MarketParams (lets you avoid maintaining oracle/irm in JSON)
  'function idToMarketParams(bytes32 id) view returns (address loanToken,address collateralToken,address oracle,address irm,uint256 lltv)',
  // Position (for vault/allocator owner). We only use supplyShares for allocation.
  'function position(bytes32 id,address user) view returns (uint256 supplyShares,uint128 borrowShares,uint128 collateral)'
]);

// ---- IRM: Adaptive Curve (per-second WAD) -----------------------------------
type IrmKind = 'perSecondWad' | 'perYearWad';
type IrmConfig = {
  address: Address;
  abi: ReturnType<typeof parseAbi>;
  functionName: string;
  kind: IrmKind;
};

const IRMS: Record<string, IrmConfig> = {
  // Use this key in markets.json ("irmKey": "IRM_ADAPTIVE")
  IRM_ADAPTIVE: {
    address: '0xD4a426F010986dCad727e8dd6eed44cA4A9b7483' as Address, // HyperEVM AdaptiveCurveIRM
    abi: parseAbi([
      // IIrm.borrowRateView(MarketParams, Market) → uint256
      // MarketParams = (loanToken, collateralToken, oracle, irm, lltv)
      // Market       = (totalSupplyAssets, totalSupplyShares, totalBorrowAssets, totalBorrowShares, lastUpdate, fee)
      'function borrowRateView((address,address,address,address,uint256),(uint128,uint128,uint128,uint128,uint128,uint128)) view returns (uint256)'
    ]),
    functionName: 'borrowRateView',
    kind: 'perSecondWad'
  }
};

// ---- Types ------------------------------------------------------------------

type MarketCfg = {
  symbol: string;
  marketId: string; // Will be validated and cast to `0x${string}` at runtime
  irmKey: string;
};

// ---- API Types --------------------------------------------------------------

type MorphoApiMarket = {
  uniqueKey: string;
  lltv: string;
  oracleAddress: string;
  irmAddress: string;
  loanAsset: {
    address: string;
    symbol: string;
    decimals: number;
  };
  collateralAsset: {
    address: string;
    symbol: string;
    decimals: number;
  };
  state: {
    borrowAssets: string;
    supplyAssets: string;
    fee: string;
    utilization: number;
    borrowApy: number;
    supplyApy: number;
    liquidityAssets: string;
  };
};

type MorphoApiResponse = {
  data: {
    markets: {
      items: MorphoApiMarket[];
    };
  };
};

// ---- Client -----------------------------------------------------------------

const client = createPublicClient({
  transport: http(RPC_URL, { timeout: 10_000 })
});

// ---- Helpers ----------------------------------------------------------------

function toNumber(bi: bigint, decimals = 18): number {
  const s = bi.toString().padStart(decimals + 1, '0');
  const i = s.slice(0, -decimals) || '0';
  const f = s.slice(-decimals);
  return Number(`${i}.${f}`); // OK on display path
}

function apyFromPerSecondWad(ratePerSecondWad: bigint): number {
  const r = Number(ratePerSecondWad) / Number(WAD);
  return Math.expm1(r * SECONDS_PER_YEAR); // e^(r*t)-1
}

async function borrowRatePerSecondWad(irmKey: string, marketId: `0x${string}`): Promise<bigint> {
  const irm = IRMS[irmKey];
  if (!irm) throw new Error(`Unknown IRM key: ${irmKey}`);

  const marketParams = await readMarketParams(marketId); // (loan, collateral, oracle, irm, lltv)
  const market = await client.readContract({
    address: MORPHO_BLUE,
    abi: MORPHO_BLUE_ABI,
    functionName: 'market',
    args: [marketId]
  }) as MarketTuple;

  // Call AdaptiveCurveIRM.borrowRateView(marketParams, market)
  const rate = await client.readContract({
    address: irm.address,
    abi: irm.abi,
    functionName: irm.functionName as any,
    args: [marketParams, market]
  }) as bigint;

  return rate; // per-second WAD
}

async function readDecimals(addr: Address): Promise<number> {
  const dec = await client.readContract({ address: addr, abi: ERC20_ABI, functionName: 'decimals' });
  return Number(dec);
}


function getVaultForLoanToken(loanToken: Address): Address {
  const loanTokenLower = loanToken.toLowerCase();
  if (loanTokenLower === WHYPE_ADDRESS.toLowerCase()) {
    return WHYPE_VAULT;
  }
  // Default to USDT0 vault for all other tokens (USDT0, etc.)
  return VAULT;
}

async function readVaultSupplyShares(marketId: `0x${string}`, vaultAddress: Address): Promise<bigint> {
  const [supplyShares] = await client.readContract({
    address: MORPHO_BLUE,
    abi: MORPHO_BLUE_ABI,
    functionName: 'position',
    args: [marketId, vaultAddress]
  }) as unknown as [bigint, bigint, bigint];
  return supplyShares;
}

function calculateAvailableLiquidity(totalSupplyAssets: bigint, totalBorrowAssets: bigint): bigint {
  return totalSupplyAssets - totalBorrowAssets;
}

// ---- API Functions ----------------------------------------------------------

async function fetchMarketsFromApi(marketIds: string[]): Promise<Map<string, MorphoApiMarket>> {
  // First, let's check if HyperEVM is supported by trying a simple query
  const testQuery = `
    query TestChains {
      chains {
        id
        network
      }
    }
  `;

  try {
    // Test if HyperEVM is supported
    const testResponse = await fetch('https://api.morpho.org/graphql', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query: testQuery })
    });

    if (!testResponse.ok) {
      throw new Error(`API test request failed: ${testResponse.status} ${testResponse.statusText}`);
    }

    const testData = await testResponse.json();
    const supportedChains = testData.data?.chains || [];
    const hyperEVMSupported = supportedChains.some((chain: any) => chain.id === CHAIN_ID);
    
    if (!hyperEVMSupported) {
      console.log(`HyperEVM (chain ID ${CHAIN_ID}) not supported by Morpho API, using on-chain fallback`);
      return new Map();
    }

    // If supported, try to fetch markets
    const query = `
      query GetMarkets($chainId: [Int!]!, $marketIds: [String!]!) {
        markets(
          where: { 
            chainId_in: $chainId,
            uniqueKey_in: $marketIds
          }
        ) {
          items {
            uniqueKey
            lltv
            oracleAddress
            irmAddress
            loanAsset {
              address
              symbol
              decimals
            }
            collateralAsset {
              address
              symbol
              decimals
            }
            state {
              borrowAssets
              supplyAssets
              fee
              utilization
              borrowApy
              supplyApy
              liquidityAssets
            }
          }
        }
      }
    `;

    const variables = {
      chainId: [CHAIN_ID],
      marketIds: marketIds
    };

    const response = await fetch('https://api.morpho.org/graphql', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query, variables })
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`API request failed: ${response.status} ${response.statusText} - ${errorText}`);
    }

    const data: MorphoApiResponse = await response.json();
    
    if (data.data?.markets?.items) {
      const marketMap = new Map<string, MorphoApiMarket>();
      data.data.markets.items.forEach(market => {
        marketMap.set(market.uniqueKey, market);
      });
      console.log(`Successfully fetched ${marketMap.size} markets from API`);
      return marketMap;
    }
    
    return new Map();
  } catch (error) {
    console.warn('Failed to fetch from Morpho API, falling back to on-chain reads:', error);
    return new Map();
  }
}

type MarketParamsTuple = readonly [Address, Address, Address, Address, bigint];
type MarketTuple = readonly [bigint, bigint, bigint, bigint, bigint, bigint];

async function readMarketParams(marketId: `0x${string}`): Promise<MarketParamsTuple> {
  return await client.readContract({
    address: MORPHO_BLUE,
    abi: MORPHO_BLUE_ABI,
    functionName: 'idToMarketParams',
    args: [marketId]
  }) as MarketParamsTuple;
}

// ---- Main -------------------------------------------------------------------

(async () => {
  const markets: MarketCfg[] = JSON.parse(await fs.readFile(path.resolve('markets.json'), 'utf8'));

  const out = {
    timestamp: new Date().toISOString(),
    chainId: CHAIN_ID,
    vault: VAULT, // Backward compatibility - USDT0 vault
    vaults: {
      usdt0: VAULT,
      whype: WHYPE_VAULT
    },
    markets: [] as any[]
  };

  // Try to fetch data from API first
  const marketIds = markets.map(m => m.marketId);
  const apiMarkets = await fetchMarketsFromApi(marketIds);
  
  console.log(`Fetched ${apiMarkets.size} markets from API, ${markets.length - apiMarkets.size} will use on-chain fallback`);

  for (const m of markets) {
    // Validate marketId format
    if (!m.marketId.startsWith('0x') || m.marketId.length !== 66) {
      throw new Error(`Invalid marketId format for ${m.symbol}: ${m.marketId}. Expected 32-byte hex string (66 chars including 0x)`);
    }
    
    const apiMarket = apiMarkets.get(m.marketId);
    
    if (apiMarket) {
      // Use API data
      const totalSupplyAssets = BigInt(apiMarket.state.supplyAssets);
      
      if (totalSupplyAssets === 0n) {
        console.log(`Skipping ${m.symbol}: totalSupplyAssets is 0 (empty market)`);
        continue;
      }

      // Check if required fields exist
      if (!apiMarket.loanAsset || !apiMarket.collateralAsset) {
        console.log(`Skipping ${m.symbol}: missing asset information in API response`);
        continue;
      }

      // Vault allocation: convert supplyShares to assets using current exchange rate
      // Need to fetch actual shares data from on-chain since API doesn't provide totalSupplyShares
      let vaultAllocationAssets: bigint | null = null;
      try {
        // Determine which vault to use based on loan token
        const vaultToUse = getVaultForLoanToken(apiMarket.loanAsset.address as Address);
        const supplyShares = await readVaultSupplyShares(m.marketId as `0x${string}`, vaultToUse);
        
        // Fetch the market tuple to get totalSupplyShares for accurate conversion
        const marketTuple = await client.readContract({
          address: MORPHO_BLUE,
          abi: MORPHO_BLUE_ABI,
          functionName: 'market',
          args: [m.marketId as `0x${string}`]
        }) as MarketTuple;
        
        const totalSupplyShares = marketTuple[1]; // totalSupplyShares from on-chain
        
        // Convert shares to assets: (supplyShares * totalSupplyAssets) / totalSupplyShares
        if (totalSupplyShares > 0n) {
          vaultAllocationAssets = (supplyShares * totalSupplyAssets) / totalSupplyShares;
        } else {
          vaultAllocationAssets = 0n;
        }
      } catch {
        vaultAllocationAssets = null;
      }

      out.markets.push({
        symbol: m.symbol,
        marketId: m.marketId,
        loan: apiMarket.loanAsset.address,
        collateral: apiMarket.collateralAsset.address,
        utilisation: apiMarket.state.utilization,
        borrowAPY: apiMarket.state.borrowApy,
        supplyAPY: apiMarket.state.supplyApy,
        availableLiquidity: toNumber(BigInt(apiMarket.state.liquidityAssets), apiMarket.loanAsset.decimals),
        vaultAllocation: vaultAllocationAssets !== null ? toNumber(vaultAllocationAssets, apiMarket.loanAsset.decimals) : null
      });
    } else {
      // Fallback to on-chain reads
      console.log(`Using on-chain fallback for ${m.symbol}`);
      
      // Get market parameters (loan token, collateral, etc.)
      const params = await readMarketParams(m.marketId as `0x${string}`);
      const loanToken = params[0]; // dynamic
      const collateralToken = params[1];
      
      // Totals (get full market data including shares)
      const marketTuple = await client.readContract({
        address: MORPHO_BLUE,
        abi: MORPHO_BLUE_ABI,
        functionName: 'market',
        args: [m.marketId as `0x${string}`]
      }) as MarketTuple;
      
      const totalSupplyAssets = marketTuple[0];
      const totalSupplyShares = marketTuple[1]; 
      const totalBorrowAssets = marketTuple[2];
      
      if (totalSupplyAssets === 0n) {
        console.log(`Skipping ${m.symbol}: totalSupplyAssets is 0 (empty market)`);
        continue;
      }
      
      // Utilization (WAD)
      const utilisationWad = (totalBorrowAssets * WAD) / totalSupplyAssets;

      // Get loan token decimals and calculate available liquidity
      const loanDecimals = await readDecimals(loanToken);
      const cash = calculateAvailableLiquidity(totalSupplyAssets, totalBorrowAssets);

      // Borrow rate (per-second WAD) from IRM
      const br_ps = await borrowRatePerSecondWad(m.irmKey, m.marketId as `0x${string}`);
      const borrowAPY = apyFromPerSecondWad(br_ps);

      // Fee from market() (6th return in tuple), WAD-scaled
      const feeWad = marketTuple[5]; // uint128 fee, WAD-scaled

      // Supply rate per-second (approx): br_ps * U * (1 - fee)
      const supplyRate_ps = (br_ps * utilisationWad) / WAD * (WAD - feeWad) / WAD;
      const supplyAPY = apyFromPerSecondWad(supplyRate_ps);

      // Vault allocation: convert supplyShares to assets using current exchange rate
      let vaultAllocationAssets: bigint | null = null;
      try {
        // Determine which vault to use based on loan token
        const vaultToUse = getVaultForLoanToken(loanToken);
        const supplyShares = await readVaultSupplyShares(m.marketId as `0x${string}`, vaultToUse);
        // Convert shares to assets: (supplyShares * totalSupplyAssets) / totalSupplyShares
        if (totalSupplyShares > 0n) {
          vaultAllocationAssets = (supplyShares * totalSupplyAssets) / totalSupplyShares;
        } else {
          vaultAllocationAssets = 0n;
        }
      } catch {
        vaultAllocationAssets = null;
      }

      out.markets.push({
        symbol: m.symbol,
        marketId: m.marketId,
        loan: loanToken,
        collateral: collateralToken,
        utilisation: Number(utilisationWad) / Number(WAD),
        borrowAPY,
        supplyAPY,
        availableLiquidity: toNumber(cash, loanDecimals),
        vaultAllocation: vaultAllocationAssets !== null ? toNumber(vaultAllocationAssets, loanDecimals) : null
      });
    }
  }

  const file = `snapshot-${new Date().toISOString().slice(0,10)}.json`;
  const outDir = path.resolve('out');
  await fs.mkdir(outDir, { recursive: true });
  await fs.writeFile(path.join(outDir, file), JSON.stringify(out, null, 2));
  await fs.writeFile(path.join(outDir, 'latest.json'), JSON.stringify(out, null, 2));
  console.log(`Wrote out/${file}`);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
