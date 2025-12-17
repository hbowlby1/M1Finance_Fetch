# This is Still a work in progress

# M1Finance Fetch

## Description

A Python application for fetching financial data from M1 Finance. This tool allows users to retrieve portfolio information, transaction history, and other financial metrics programmatically.

## Features

- Fetch portfolio data
- Retrieve transaction history
- Authenticate with M1 Finance API
- Export data in various formats

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

4. Follow the prompts to authenticate and fetch data.

## Configuration

- Update authentication details in `auth/authenticate.py`
- Configure API endpoints as needed

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This tool is not officially affiliated with M1 Finance. Use at your own risk.