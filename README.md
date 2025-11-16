# Morpho Daily

A TypeScript tool for fetching and tracking daily snapshots of Morpho Blue market data on HyperEVM. The tool collects market metrics (APY, utilization, liquidity) and vault allocations, saving them as JSON snapshots.

## Features

- Fetches market data from Morpho API (with on-chain fallback)
- Tracks vault allocations across multiple markets
- Calculates utilization, borrow APY, supply APY, and available liquidity
- Generates daily timestamped snapshots
- Supports multiple vaults (USDT0 and WHYPE)

## Prerequisites

- Node.js (v18 or higher)
- npm or yarn
- Access to a HyperEVM RPC endpoint

## Installation

```bash
npm install
```

## Configuration

### Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# Required
HYPEREVM_RPC_URL=https://your-hyperevm-rpc-url
MORPHO_BLUE=0xYourMorphoBlueContractAddress
VAULT_ADDRESS=0xYourVaultAddress
# OR use PUBLIC_ALLOCATOR instead of VAULT_ADDRESS

# Optional
MORPHO_LENS=0xYourMorphoLensContractAddress  # If you have a Lens contract deployed
```

### Markets Configuration

Edit `markets.json` to specify which markets to track:

```json
[
  {
    "symbol": "USDT0–kHYPE",
    "marketId": "0xc5526286d537c890fdd879d17d80c4a22dc7196c1e1fff0dd6c853692a759c62",
    "irmKey": "IRM_ADAPTIVE"
  }
]
```

- `symbol`: Human-readable identifier for the market
- `marketId`: 32-byte hex string (66 characters including `0x`) identifying the Morpho Blue market
- `irmKey`: Key matching an IRM configuration in the code (currently supports `IRM_ADAPTIVE`)

## Usage

### Run Once

```bash
npm run fetch
```

or

```bash
npm start
```

### Automated Daily Runs

Use the provided shell script with a cron job:

```bash
# Make the script executable
chmod +x run_fetch.sh

# Add to crontab (runs daily at 00:00)
0 0 * * * /path/to/morpho-daily/run_fetch.sh
```

## Output

Snapshots are saved in the `out/` directory:

- `snapshot-YYYY-MM-DD.json`: Daily timestamped snapshots
- `latest.json`: Most recent snapshot (overwritten on each run)
- `fetch.log`: Execution logs (if using the shell script)

### Output Format

```json
{
  "timestamp": "2025-11-14T12:00:00.000Z",
  "chainId": 999,
  "vault": "0x...",
  "vaults": {
    "usdt0": "0x...",
    "whype": "0x..."
  },
  "markets": [
    {
      "symbol": "USDT0–kHYPE",
      "marketId": "0x...",
      "loan": "0x...",
      "collateral": "0x...",
      "utilisation": 0.65,
      "borrowAPY": 0.12,
      "supplyAPY": 0.08,
      "availableLiquidity": 1000000.5,
      "vaultAllocation": 500000.25
    }
  ]
}
```

## Adapting for Your Use Case

### Adding New IRMs

Edit `src/fetch-morpho.ts` and add to the `IRMS` object:

```typescript
const IRMS: Record<string, IrmConfig> = {
  IRM_ADAPTIVE: { /* existing config */ },
  IRM_YOUR_NEW: {
    address: '0x...' as Address,
    abi: parseAbi([/* your IRM ABI */]),
    functionName: 'borrowRateView',
    kind: 'perSecondWad' // or 'perYearWad'
  }
};
```

Then reference it in `markets.json` with `"irmKey": "IRM_YOUR_NEW"`.

### Supporting Different Chains

1. Update `CHAIN_ID` constant in `src/fetch-morpho.ts`
2. Update RPC URL in `.env`
3. Verify Morpho API supports your chain (or rely on on-chain fallback)
4. Update contract addresses (Morpho Blue, IRMs, vaults)

### Adding New Vaults

1. Add vault address constant:
```typescript
const YOUR_VAULT = '0x...' as Address;
```

2. Update `getVaultForLoanToken()` to map loan tokens to your vault:
```typescript
function getVaultForLoanToken(loanToken: Address): Address {
  // Add your logic here
  if (loanToken.toLowerCase() === YOUR_TOKEN.toLowerCase()) {
    return YOUR_VAULT;
  }
  // ... existing logic
}
```

3. Add to output structure in the main function:
```typescript
vaults: {
  usdt0: VAULT,
  whype: WHYPE_VAULT,
  yours: YOUR_VAULT
}
```

### Using a Morpho Lens

If you have a Lens contract deployed, set `MORPHO_LENS` in `.env` and update the `LENS_ABI` in `src/fetch-morpho.ts` with your Lens contract's ABI. The code will prefer Lens reads when available.

## Project Structure

```
morpho-daily/
├── src/
│   └── fetch-morpho.ts    # Main fetch script
├── out/                    # Output directory (generated)
├── markets.json            # Market configuration
├── run_fetch.sh            # Shell script for automation
├── package.json
├── tsconfig.json
└── README.md
```

## Development

### Build

```bash
npm run build
```

### Type Checking

The project uses TypeScript with strict mode enabled. Run the TypeScript compiler to check for errors:

```bash
npx tsc --noEmit
```

## Troubleshooting

- **"Missing env" error**: Ensure all required environment variables are set in `.env`
- **"Invalid marketId format"**: Market IDs must be 66-character hex strings starting with `0x`
- **API fallback**: If Morpho API doesn't support your chain, the tool automatically falls back to on-chain reads
- **Empty markets**: Markets with `totalSupplyAssets = 0` are skipped

## License

Private project - see package.json for details.
