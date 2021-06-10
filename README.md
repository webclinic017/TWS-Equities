# TWS - Equites Data Downloader

---

![GitHub top language](https://img.shields.io/github/languages/top/amanoberoik2q/TWS-Equities?color=darkgreen&logo=python&logoColor=yellow&style=plastic)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/amanoberoik2q/TWS-Equities?logo=git&style=plastic)
![GitHub all releases](https://img.shields.io/github/downloads/amanoberoik2q/TWS-Equities/total?logo=github&style=plastic)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/amanoberoik2q/TWS-Equities?color=yellow&style=plastic)
![GitHub](https://img.shields.io/github/license/amanoberoik2q/TWS-Equities?style=plastic)


---

Owner: **K2Q Capital Limited**  
Author: **Mandeep Singh**  
Updated: **February 16th 2021**  
Version: **1.1.0**

---

### About this project:
TWS Equities Data Downloader is a Python project written around Interactive Broker's API that provides easy access to a stock bar-data from Japanese Stock Exchange. It is designed to be flexible enough to accomodate various market parameters and provide data ready to be consumed in an organized fashion by means of JSON objects.

**NOTE:** Currently the program has only been tested on Japanese Market.

---

### Prerequisites:
Before users try to interact with the code, they must ensure that following criteria are met:

> - Install Python 3.6 or later.
> - Use Linux or Mac OS machine, code has not yet been tested on Windows machines..
> - Have some basic knowledge about working with a Command Line Interfaces(CLIs).

Once the above requirements are satisfied, please follow the steps mentioned below to setup the project on your local machine:

#### **Cloning the project:**
Run the following command to clone the project into your local machine:
> **`git clone https://github.com/sudoMode/TWS-Project.git`**

#### **Virtual Environment:**
The project is dependent on a few 3rd-party libraries that include:
> - [Alive-Progress](https://pypi.org/project/alive-progress/)
> - [IBAPI](https://interactivebrokers.github.io/tws-api/introduction.html)
> - [Pandas](https://pypi.org/project/pandas/)

- Alive-Progress & Pandas are available from [PyPI](https://pypi.org/) and can be installed directly using "pip" or "easy_install".
- IBAPI is owned and maintained by Interactive Brokers and is **not available** on PyPI, so please [follow these instructions](http://interactivebrokers.github.io/#) to install that library as per your environment.
- Project contains a "requirements.txt" file that can be used to setup your new virtual environment, but still IBAPI must be built from scratch and installed in the new environment.

#### **Launch IB's Trader Work Station(TWS):**
User must launch Trader Workstation prior to interacting with the code, this is necessary because IB API will be hosted on your local machine once you're logged into TWS. Code uses default values to connect with the API which can be tweaked from Global API Settings in TWS.
```
    DEFAULT_HOST = "127.0.0.1"
    DEFAULT_PORT = 7497
```

**NOTE:**
- To allow connections from other hosts, add the requesting to host address to "Trusted IPs" under "Edit -> Global Configuration -> API -> Settings".
- Developers must only log into **TWS Paper Trading** account.

---

### How to use this project?
User can start interacting with the code via given Command Line Interface(CLI). "tws_equities" package provides a nice and clean way to issue commands to the CLI, here's are a couple of sample command:
> **`python -m tws_equities -h`**(view help menu)  
> **`python -m tws_equities run`**(trigger a compete run with default list of tickers)  
> **`python -m tws_equities run --end-date 20210119 --end-time 09:01:00 tickers -l 1301`**(choose custom
parameters)

CLI no longer outputs data to console because the data object retrieved from the API can be huge. Though the downloaded data will be cached inside the directory called "historical_data", which would again be used to generate a final CSV file. User can still access raw data from "historical_data" which would look something like this:

> Successful extraction:
```
    {
      "1301": {
        "bar_data": [
          {
            "average": 2951.0,
            "close": 2951.0,
            "count": 1,
            "high": 2951.0,
            "low": 2951.0,
            "open": 2951.0,
            "session": 1,
            "time_stamp": "20210104  09:00:00",
            "volume": 300
          }
        ],
        "meta_data": {
          "_error_stack": [],
          "attempt": 1,
          "end": "20210119  09:01:00",
          "start": "20210119  09:01:00",
          "status": true,
          "ecode": 1301
        }
      }
    }
```

> Failed extraction:
```
    {
      "1301": {
        "bar_data": [],
        "meta_data": {
          "_error_stack": [
            {
                "code": -1,
                "message": "data extraction request timed out after: 3 seconds."
            },
            {
                "code": 162,
                "message": "Historical Market Data Service error message:HMDS query returned no data: 1301.
                T@SMART Trades"
            }
          ],
          "attempt": 1,
          "end": "20210119  09:01:00",
          "start": "20210119  09:01:00",
          "status": true,
          "ecode": 1301
        }
      }
    }

```

---

### Available commands & options:
The CLI provides four major commands to work with:
#### Run:

- **Desctiption:**  
  This command can be used to trigger a complete end to end run, which would include data download, conversion to CSV & finally metrics generation.


- **Usage:**
> `python -m tws_equities run`

- **Options:**
> - **--start-date/-sd:** Date from which the extraction should start, expected format is "YYYYMMDD".(Defaults to current date)
> - **--end-date/-ed:** End date for the extraction process, expected format is "YYYYMMDD".(Defaults to current date)
> - **--end-time/-ed:** End time for the bar-data, expected format is "HH:MM:SS"(24-hour format).(Defaults to "15:01:00" which is market closing time.)
> - **--duration/-d:** Duration for which data is needed, default value is "1 D" i.e. 1 day. It is recommended to use start-date option to pull data for mutliple days rather increasing the duration as it will return significantly larger data objects.
> - **--bar-size/-b:** Granularity of the data extracted, default is "1 min".
> - **--what-to-show/-w:** Available options for kind of data to pull, currently only "TRADES" is supported.
> - **--use-rth/-u:** To or not to pull data from outside regular trading hours, default is 1.

- **Sub-Commands:**
> **tickers:**
>> - **Description:**  
     This command allows the user to pass in a custom ticker input to the prorgam, it provides 3 different ways to do that:
>>> - **--list / -l:** Accepts a list of ticker IDs separated by white-space.
>>> - **--file / -f:** Accepts a CSV file path as input to read tickers IDs.
>>> - **--url / -u:** Will accept a google sheet URL as input to read tickers IDs, this feature is still under development and not available for usage yet.
>> - **Usage:**
>>> `python -m tws_equities run tickers -l 1301 1302 1303`  
>>> `python -m tws_equities run tickers -f ~/Users/files/test_tickers.csv`  
>>> `python -m tws_equities run tickers -u https://sheets.google.com/?sheet_id=123xyz`

#### Download:
- **Description:**
  This command allows the user to only download and store the bar-data. All options & sub-command available under "Run" section are applicable here as well.
  Kindly run the following command for more information:
> **`python -m tws_equities download -h`**

#### Convert:
- **Description:**
  This command allows the user to trigger data conversion from JSON to CSV format, please note that this command already assumes that user has downloaded the data from TWS.
  Kindly run the following command for more information:
> **`python -m tws_equities convert -h`**

#### Metrics:
- **Description:**
  This command will generate the data-extraction metrics for a given date, this feature is still a work in progress and not avilable for usage via CLI calls.
  Kindly run the follo command for more information:
> **`python -m tws_equities metrics -h`**

---

### Sample commands:
- Trigger a run for current date with default parameters:
>```python -m tws_equities run```

- Trigger a run for a specific date:
>```python -m tws_equities run -ed 20201213```

- Trigger a run for a date range:
> ```python -m tws_equities run -sd 20210110 -ed 20210118```

- Trigger a run with custom parameters:
> - Modify bar-size:
>> ```python -m tws_equities run -b "5 mins"```

> - Modify duration of data extracted:
>> ```python -m tws_equities run -d "1 Day"```

> - Modify input file:
>> ```python -m tws_equities run tickers -f ~/Users/downloads/test_file.csv```

**NOTE:**

- Please use the "--help / -h" option against the CLI to browse through the documentation and to view all
  acceptable input values.
- All these paramters can be used in any combination, but it worth noting that having a large value for
  duration(ex: 10 Day) with a highly-granular bar-size(ex: 1 min) will lead to significantly larger output
  CSV file, which would be resource intensive for the program and also much harder to comprehend in any
  spreadsheet viewer. A better option would be to distribute your extraction across a date-range by using
  start-date option.
- When using any external CSV file as input, user must ensure that the file has a column in of
  the following values "ecode / e_code / code / ticker / ticker_id" where ticker IDs are stored
  (case-insensitive).

---

### How to contribute to this project?
This project follows a pretty straightforward branching strategy that allows new developers to contribute in a very intuitive manner. Branching strategy is as follows:
> `dev --> test --> master`

- Master branch is the default one and used to create new release links.
- Test branch contains the new features for which developement has been completed and are being tested currently.
- Dev branch always has the latest code and all the experimental features that are under development.

Developer are recommended to follow the steps below to make contribution to the project:

> 🐞 Raise a bug:  
Users can drop an email to (to be added...) with a detailed description of the problem.
It is recommended that users attach screenshot of the problem and also provide input command & output received from the CLI.

> 💻 Contribute to code:
> - Developers are recommended to fork this Github repository and code new features in their in your
    repository.
> - Create a new branch by the name of the feature you want to develop.
> - Once changes are done, commit them with proper messages.
> - Push the original branch back to origin and create a pull request.
> - Changes will be reviewed and merged after approval from the repository owner.

> 📖 Contibuting to documentation:
> - If you spot a problem with the project documentation and wish to report it, please follow the steps
    mentioned under Raise a bug section.
> - If you want to update the documentation yourself:
>> - Fork the dev branch and make your changes.
>> - Publish your changes(with proper commit messages) and create a pull request back into the dev branch.

---

### Contact information:
> Project Owner:
>> - **[Aman Oberoi](mailto:aman.oberoi@k2qcapital.com?subject=Regarding%20TWS-Equities)**

> Developers:
>> - **[Mandeep Singh](mailto:mandeep@amakaan.com?subject=Regarding%20TWS-Equities)**

---

### License:
MIT
