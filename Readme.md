# TSETMCE Stock Market Data Analysis Project

<img src="logo.jpg" style="display: block; margin: auto; height: 500px; width: 500px;">


## About the Project

This project is developed using Python and consists of four main components:

### 1. GetExcel.py

This module sends requests to the TSETMC ([.:TSETMC:. :: مدیریت فناوری بورس تهران](https://www.tsetmc.com/)) and downloads the data to a stage folder.

### 2. ConvertToCSV.py

This module converts the downloaded files to CSV files and stores them in a datalake folder.

### 3. Analysis.py

This module analyzes the data and represents the results in an HTML file.

- All the modules above have customizable features and accept arguments from the terminal using the `argparser` library.

### 4. Logger

This module is developed using python logging module and logs warnings, errors, and criticals to a `{caller-name}-error.log` file and informational messages to a `{caller-name}-info.log` file in the log folder made in the working directory.

## Usage

Each module can be run from the terminal with arguments to customize their behavior. For more details on the arguments for each module, please refer to the documentation within each Python file.

## Installation

- All the required modules are listed in the `requirements.txt` file. 

```Shell
pip install -r requirements.txt
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

MIT
