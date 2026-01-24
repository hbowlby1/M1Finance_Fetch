# M1Finance Fetch

<div align="center">
  <img src="logo.svg" alt="M1Finance Fetch Logo" width="200" height="200">
</div>

> **Note:** This project is functional and can fetch open tax lots, closed tax lots, and holdings data, exporting them to CSV files. Google Sheets integration enables automated data transfer and worksheet creation with proper formatting.

## Description

A Python application for fetching financial data from M1 Finance. This tool allows users to retrieve portfolio information, including open and closed tax lots, current holdings, and other financial metrics programmatically. Optional Google Sheets integration enables automatic data synchronization, creating dedicated worksheets with currency and percentage formatting.

## Features

- Fetch open and closed tax lots data
- Retrieve current holdings/portfolio positions
- Authenticate with M1 Finance API (not official)
- Export data to CSV files
- Google Sheets integration for automated data transfer and worksheet creation
- Automatic currency and percentage formatting in Google Sheets
- Comprehensive error handling and logging
- Pagination support for large datasets

## Installation

Prerequisites:
- Docker Desktop (for the recommended Docker workflow; includes Docker Compose v2)
- Python 3.10+ (only if you plan to run locally)

Clone the repository:
```bash
git clone <repository-url>
cd M1Finance_Fetch
```

## Usage

### Docker (recommended)

1. Build and start the app:
   ```bash
   docker compose up --build
   ```
   On first run, the app creates `./config/.env` and `./config/state.json` on your machine and then exits.
   If you only have `docker-compose` (v1), use that instead of `docker compose`.

2. Edit the config on your machine (config folder or via terminal):
   ```bash
   # Open ./config/.env and ./config/state.json in your editor
   ```

3. Run again:
   ```bash
   docker compose up
   ```

### Local (development)

1. Install and ensure the virtual environment is activated:
    ```bash
    python -m venv vir
    .\vir\Scripts\activate # windows (PowerShell)
    source vir/bin/activate # mac/linux
    ```
2. Ensure you are in the virtual environment, then install dependencies:
   ```bash
   pip install -r requirements.txt
   
   ```
3. Run the program once to generate blank env and state.json files for later
    ```bash
    python main.py
    ```
4. The app creates `./config/.env` and `./config/state.json` if they do not exist. Update them and run again.

### Environment Variables (applies to both)

- Add your M1 email and password to `.env`.
- For `MFA_AUDIENCE`, set to `true` if you have multifactor authentication enabled, otherwise leave as `false`.
- Generate a UUID for `SEGMENT_ID`:
  Run this command in your terminal:
  ```bash
  python -c "import uuid; print(uuid.uuid4())"
  ```
  Copy the output and add it as `SEGMENT_ID` in your `.env` file.
- Get your Account ID from M1 Finance:
  - Log into M1 Finance.
  - Click your name towards the bottom of the left side menu.
  - Click "Accounts" at the top nav bar.
  - Copy your Number for the Account you want to Track.
  - Put that number in `ACCOUNT_ID` in your `.env` file.
- Get your other Account ID from M1 Finance:
  - The easiest way to do this is to go to the holdings page of the account you are interested in tracking.
  - The URL will usually look like: `dashboard.m1.com/invest/XXXXXXXXXXXXX=/holdings`
  - The string between `invest/` and `/holdings` is what you want to copy and put in `OTHER_ACCOUNT_ID`.

### Google Sheets Integration Setup

- Set `ENABLE_GOOGLE_SHEETS_INTEGRATION="true"` in your `.env` file
- Set up your GCP project following Gspread's instructions:
  - [Gspread](https://docs.gspread.org/en/latest/oauth2.html#for-bots-using-service-account)
- Download your service account JSON and save it as `./config/serviceAccount.json`
- Remove any `serviceAccount.json` in the project root to avoid confusion; the app reads from `./config/`
- If you rename the credentials file, update `CREDENTIALS_PATH` in your `.env` file
- It already tells you to create a spreadsheet and then share it with the service email. This is required for sheets integration. Please give it a name of M1 Finance Management. Or if you want to name it yourself update the value for SPREADSHEET_NAME in state.json to your file name.

The fetched data will be automatically saved as CSV files in the `CSV` folder if enabled in the state.json file. If Google Sheets integration is enabled, data will also be uploaded to dedicated worksheets with automatic formatting.

## Output

The application generates CSV files containing the fetched financial data. Files are saved in the `CSV` folder within the project directory:

- `open_tax_lots.csv`: Contains open tax lots data including symbols, quantities, cost basis, and unrealized gain/loss information.
- `closed_tax_lots.csv`: Contains closed tax lots data including symbols, quantities, cost basis, realized gain/loss, and close dates.
- `holdings.csv`: Contains current portfolio holdings with position details, values, and performance metrics.

### Google Sheets Integration

When Google Sheets integration is enabled, the application automatically creates or updates dedicated worksheets in your specified Google Spreadsheet:

- **Holdings Worksheet**: Displays current portfolio positions with formatted currency columns (e.g., average share price, total cost, current value, unrealized gain) and percentage columns (e.g., unrealized gain percent, maintenance margin percent).
- **Open Tax Lots Worksheet**: Shows open tax lots with proper data type handling and formatting.
- **Closed Tax Lots Worksheet**: Presents closed tax lots data, including realized gains/losses.

Worksheets are automatically formatted with USD currency patterns and percentage displays for better readability. Data is cleared and refreshed on each run to ensure up-to-date information.

### Create a Holdings Pie Chart (Google Sheets)

1. Open your spreadsheet and go to the `Securities Info` sheet.
2. Click the `Insert` dropdown in the top toolbar.
3. Select `Chart`. Google Sheets will auto-generate a pie chart of your holdings.
4. Customize the chart as desired using the chart editor.

## Configuration

The application uses environment variables for all configuration. When using Docker, the app creates `./config/.env` on first run. For local development, the app uses the same `/app/config/.env` path. The file should contain:

```env
# M1 Finance Authentication
EMAIL=your_m1_email@example.com
PASSWORD=your_m1_password
MFA_AUDIENCE=false
SEGMENT_ID=your_segment_id
ACCOUNT_ID=your_account_number
OTHER_ACCOUNT_ID=your_encoded_account_id
```

<!-- ### Feature Toggles

Control application behavior handled in state.json:

- `ENABLE_GOOGLE_SHEETS_INTEGRATION`: Enable/disable Google Sheets authentication and data transfer (default: false)
- `CREDENTIALS_PATH`: Path to your Google API credentials file (default: credentials.json) -->

### State Configuration

The application also uses a `state.json` file for runtime settings that can be modified without restarting. This file is automatically created with default values if it doesn't exist at `./config/state.json`. It controls:

- `ENABLE_GOOGLE_SHEETS_INTEGRATION`: Whether to enable Google Sheets integration (overrides .env if set)
- `CREATE_NEW_SPREADSHEET`: Whether to create a new spreadsheet (currently not in use due to API limitations)
- `SPREADSHEET_NAME`: Name of the Google Spreadsheet to use/update
- `CREATE_CSV_FILES`: Whether to generate CSV files
- `GENERATE_TAX_LOTS_SHEETS`: Whether to create tax lots worksheets in Google Sheets
- `USE_DATABASE`: A feature coming soon that tracks stocks via a simple SQLITE database for a future RAG architecture plan.

Modify `./config/state.json` to customize behavior on the fly. The file is checked and created by the `checkForState` module on startup.

### Environment Template

On first run, the app creates `./config/.env` with placeholder values for all required environment variables. Fill in your actual credentials and settings, then run again.

## Project Structure

```
M1Finance_Fetch/
├── auth/
│   ├── authenticate.py          # Authentication classes for M1 and Google
│   └── __init__.py
├── checkForState/
│   ├── checkForState.py         # Manages state.json creation and defaults
│   └── __init__.py
├── fetch_csv/
│   ├── fetch_csv.py             # Data fetching logic
│   └── __init__.py
├── generateCSV/
│   ├── generateCSV.py           # CSV generation utilities
│   └── __init__.py
├── spreadsheets/
│   ├── spreadsheetManager.py    # Google Sheets integration and data upload
│   └── __init__.py
├── CSV/                         # Generated CSV files (auto-created)
├── main.py                      # Main application entry point
├── config/                      # Host config folder (contains .env and state.json)
├── blank.env                    # Optional template for manual/local use
├── logo.svg                     # Project logo
├── README.MD                    # This file
├── LICENSE                      # MIT License
├── security.md                  # Security policy
└── .gitignore                   # Git ignore rules
```

## Upcoming Features

### In Development
- Comprehensive Logging: Structured logging with configurable levels
- Data Validation: Schema validation for API responses

### Planned
- CLI Interface: Command-line options for different operations
- Caching System: API response caching to reduce calls and improve performance
- Rate Limiting: Intelligent rate limiting to prevent API blocking
- Unit Tests: Comprehensive test suite
- Video Tutorial: Step-by-step usage guide

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Test your changes thoroughly
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

<!--### Development Setup

 1. Follow the installation steps above
2. Install additional development dependencies:
   ```bash
   pip install pytest black flake8 mypy
   ```
3. Run tests: `pytest`
4. Format code: `black .`
5. Lint code: `flake8` -->

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This tool is not officially affiliated with M1 Finance. Use at your own risk. Always ensure your credentials are secure and never commit sensitive information to version control.

## Support

- **Issues**: [GitHub Issues](https://github.com/hbowlby1/M1Finance_Fetch/issues)
- **Discussions**: [GitHub Discussions](https://github.com/hbowlby1/M1Finance_Fetch/discussions)
- **Security**: See SECURITY.md for security-related concerns
