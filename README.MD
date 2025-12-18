# M1Finance Fetch

> **Note:** This project is functional and can fetch open tax lots data.

## Description

A Python application for fetching financial data from M1 Finance. This tool allows users to retrieve portfolio information, including open and closed tax lots, current holdings, and other financial metrics programmatically.

## Features

- Fetch open and closed tax lots data
- Retrieve current holdings/portfolio positions
- Authenticate with M1 Finance API
- Export data to CSV files

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd M1Finance_Fetch
   ```

2. **Set up virtual environment:**
   ```bash
   python -m venv vir
   vir\Scripts\activate  # On Windows
   ```

3. **Install dependencies:**
   ```bash
   pip install -r install.txt
   ```

   Note: The `install.txt` file contains the frozen requirements from `pip freeze`.

## Usage

1. Ensure the virtual environment is activated.
2. Run the main script:
   ```bash
   python main.py
   ```

3. Set up environment variables:
   - Rename `blank.env` to `.env`.
   - Add your M1 email and password to `.env`.
   - For `MFA_AUDIENCE`, set to `true` if you have multifactor authentication enabled, otherwise leave blank.
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

4. Follow the prompts to authenticate and fetch data.
5. The fetched data will be automatically saved as CSV files in the `CSV` folder.

## Output

The application generates CSV files containing the fetched financial data. Files are saved in the `CSV` folder within the project directory:

- `open_tax_lots.csv`: Contains open tax lots data including symbols, quantities, cost basis, and unrealized gain/loss information.
- `closed_tax_lots.csv`: Contains closed tax lots data including symbols, quantities, cost basis, realized gain/loss, and close dates.
- `holdings.csv`: Contains current portfolio holdings with position details, values, and performance metrics.

## Configuration

- Update authentication details in `auth/authenticate.py`
- Configure API endpoints as needed

## Upcoming Features
 - function to transfer CSV data to Google Sheets (requires)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This tool is not officially affiliated with M1 Finance. Use at your own risk.