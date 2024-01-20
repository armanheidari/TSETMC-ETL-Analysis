import jdatetime
import re
import os
import sys
import requests
from pathlib import Path
from typing import Union, Tuple, Optional
from argparse import ArgumentParser
from Logger import Logger


def validate_path(path: str) -> None:
    '''
        Checks if the path is valid

        - - -
        ### Parameters
        - `path`: The path to be validated

        - - -
        ### Return
        - `None`: Raises an Exception if the path is not correct

    '''
    try:
        Path(os.path.abspath(path))
    except Exception as e:
        raise ValueError(
            f"path format is not correct.\nPath: {path}")


def validate_date(date: str) -> Tuple[bool, Union[re.match, None]]:
    '''
        Checks if the date is a valid date

        - - -
        ### Parameters
        - `date`: str date to be vaidated

        - - -
        ### Return
        - `bool`: Indicates the date validation result
        - `re.match`: The date matched pattern
    '''
    pattern = re.compile(
        r"^(1[34]\d{2})-(0?\d|1[012])-(0?\d|[12]\d|3[01])$")
    if pattern.match(date):
        return True, pattern.search(date)
    else:
        return False, None


def process_date(date: str) -> jdatetime.date:
    '''
        Converts the str date to a `jdatetime.date` object if it is a valid date

        - - -
        ### Parameters
        - `date`: str date to be processed

        - - -
        ### Return
        - `jdatetime.date`: The processed date

    '''
    if (compiled_date := validate_date(date))[0]:
        return jdatetime.date(year=int(compiled_date[1].group(1)),
                              month=int(compiled_date[1].group(2)),
                              day=int(compiled_date[1].group(3)))
    else:
        raise ValueError(
            f"Invalid date format {date}. Expected format: YYYY-MM-DD.")


def does_file_exists(path: str) -> bool:
    '''
        Checks whether the file exists or not

        - - -
        ### Parameters
        - `path`: The file path

        - - -
        ### Return
        - `bool`: Indicate the file search result

    '''
    return os.path.isfile(path)


def get_file_name(date: jdatetime.date) -> str:
    '''
        Returns the excel file name associated with the date

        - - -
        ### Parameters
        - `date`: The date of the file

        - - -
        ### Return
        - `str`: The file name

    '''
    return str(date) + ".xlsx"


def make_dir(path: str) -> None:
    '''
        Makes the directory if it does not already exist

        - - -
        ### Parameters
        - `path`: The path to the directory

    '''
    if not os.path.isdir(path):
        os.mkdir(path)


def is_future(date: jdatetime.date) -> bool:
    '''
        Checks whether the date is a future date or not

        - - -
        ### Parameters
        - `date`: The date to be checked

        - - -
        ### Return
        - `bool`: Indicates the date status

    '''
    return date > jdatetime.date.today()


def save_excel(response: requests.Response, path: str) -> None:
    '''
        Saves the Excel file recieved from the TSETMC response

        - - -
        ### Parameters
        - `response`: The response from TSETMC
        - `path`: The path to the Excel file

    '''
    with open(path, mode="wb") as f:
        f.write(response.content)


def single_request(date: jdatetime.date, path: str) -> None:
    '''
        Sends the request associated with date and handles TSETMC response. 
        Then saves the file to the provided file path using `save_excel()`    

        - - -
        ### Parameters
        - `date`: The request date
        - `path`: The path to the file

    '''
    if date.weekday() in {5, 6}:
        return

    url = f"http://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx?d={str(date)}"

    response = requests.get(url)

    if response.status_code == 200:
        save_excel(response, path)
        logger.info(
            f"The {get_file_name(date)} saved successfuly.\nPath: {path}")
    else:
        response.raise_for_status()


def requests_handler(start_date: str, end_date: str, stage_path: Optional[str] = None) -> None:
    '''
        Handles the requests from `start_date` to `end_date`

        - - -
        ### Parameters
        - `start_date`: The beginning of the range 
        - `end_date`: The end of the range 

    '''
    end_date = process_date(end_date)
    start_date = end_date if start_date is None else process_date(start_date)

    if start_date > end_date:
        raise ValueError(
            f"Start date {str(start_date)} is after the end date {str(end_date)}")

    if stage_path is None:
        stage_path = os.path.join(os.getcwd(), "Stage")
    else:
        try:
            validate_path(stage_path)
        except ValueError as ve:
            raise ValueError(f"Stage {str(ve)}")

    if is_future(start_date):
        raise ValueError(
            f"Invalid date(s). The {str(start_date)} is a future date.")

    make_dir(stage_path)

    while start_date <= end_date:
        if is_future(start_date):
            logger.warning("Reached the current date. Can not go further.")
            break

        file_path = os.path.join(stage_path, get_file_name(start_date))

        if does_file_exists(file_path):
            logger.info(f"The {get_file_name(start_date)} already exists.")
            start_date += jdatetime.timedelta(days=1)
            continue

        try:
            single_request(start_date, file_path)

        except requests.exceptions.ConnectionError as cerr:
            logger.error(
                f"A Connection error occured while requesting the {get_file_name(start_date)} file.\n{str(cerr)}")

        except requests.exceptions.HTTPError as herr:
            logger.error(
                f"An HTTP error occured while requesting the {get_file_name(start_date)} file.\n{str(herr)}")

        except requests.exceptions.RequestException as rerr:
            logger.error(
                f"An error occured while requesting the {get_file_name(start_date)} file.\n{str(rerr)}")

        start_date += jdatetime.timedelta(days=1)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Downloads the market data from TSETMC.")

    parser.add_argument('-p', '--path', type=str,
                        help="The path to the stage folder (do not specify if you want to create a stage directory in the current path)")
    parser.add_argument('-s', '--start', type=str,
                        help="Start date (do not specify if you want to get the data only for one day)")
    parser.add_argument('end', type=str, help="End date")

    try:
        logger = Logger()
        args = parser.parse_args()

    except SystemExit as se:
        logger.critical(f"System exit exception occured.")
        os.system("pause")
        sys.exit(se.args[0])

    except Exception as e:
        logger.critical(f"Unkown exception occured. {str(e)}")
        sys.exit(0)

    stage_path = args.path
    start_date = args.start
    end_date = args.end

    try:
        requests_handler(start_date, end_date, stage_path)

    except ValueError as v:
        logger.critical(str(v))
        sys.exit(v.args[0])

    except Exception as e:
        logger.critical(f"Unkown exception occured. {str(e)}")
        sys.exit(0)
