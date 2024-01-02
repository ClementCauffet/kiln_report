# Kiln Report Technical Test

This program is a component of Kiln company's recruitment process. Its aim is to utilize Kiln's API to retrieve information regarding staking for one of its clients.

## Installation

### Python Version
Ensure you have **Python version 3.11** or higher installed.

### Required Packages

Install the necessary packages using pip:

```bash
pip install gspread oauth2client
pip install requests
```

## Setup

Replace **--YOUR GOOGLE SHEET NAME--** & **--YOUR SOURCE FILE NAME--** with corresponding names.

To utilize this tool effectively, ensure you have the following:

- **Google Credentials**: Obtain the `credentials.json` file from Google API Console for authentication.
- **Kiln API Key**: You'll need the Kiln API key placed at the root of your project.

## Usage
Run the following command with your address collection to start exploring staking rewards

```
python kiln_test.py
```