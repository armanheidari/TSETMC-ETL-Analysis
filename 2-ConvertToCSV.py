import os
import sys
import pandas as pd
from pathlib import Path
from glob import glob
from typing import Union, List, Optional
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


def get_excel_files(stage_path: str) -> List[str]:
    '''
        Gets all the Excel files in the `stage_path`

        - - -
        ### Parameters
        - `stage_path`: The stage path to be searched

        - - -
        ### Return
        - `List[str]`: A list of all Excel files in the provided stage path

    '''
    return glob(stage_path + "/*.xlsx")


def delete_excel(file_path: str) -> None:
    '''
        Deletes the excel located at `file_path`

        - - -
        ### Parameters
        - `file_path`: The file path to be deleted

    '''
    os.remove(file_path)


def path_to_file_name(file_path: str) -> str:
    '''
        Converts the file path to its name 

        - - -
        ### Parameters
        - `file_path`: The file path to be converted

        - - -
        ### Return
        - `str`: The name of the file

    '''
    return os.path.basename(file_path).split(".")[0]


def make_dir(path: str) -> None:
    '''
        Makes the directory if it does not already exist

        - - -
        ### Parameters
        - `path`: The path to the directory

    '''
    if not os.path.isdir(path):
        os.mkdir(path)


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


def convert_to_csv(file_path: str, dl_path: str, delete: bool) -> None:
    '''
        Converts an Excel file from the TSETMC to csv, and deletes the Excel file if the `delete` flag is set

        - - -
        ### Parameters
        - `file_path`: The path to the Excel file
        - `dl_path`: The path to the datalake where the csv files are stored
        - `delete`: Indicates whether to delete the Excel or not

    '''
    df = pd.read_excel(file_path, skiprows=[1])
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    file_name = path_to_file_name(file_path)

    if df.shape[0] == 0:
        delete_excel(file_path)
        logger.info(
            f"The empty {file_name}.xlsx deleted successfuly.\nPath:{file_path}")
    else:
        csv_file = f"{file_name}.csv"
        csv_path = os.path.join(dl_path, csv_file)

        df.to_csv(csv_path, index=False)
        logger.info(f"The {csv_file} saved successfuly.\nPath: {csv_path}")

        if delete:
            delete_excel(file_path)
            logger.info(
                f"The {file_name}.xlsx deleted successfuly.\nPath: {file_path}")


def conversion_handler(stage_path: Optional[str] = None, dl_path: Optional[str] = None, delete: Optional[bool] = False) -> None:
    '''
        Converts all the Excel files in the `stage_path` to csv files and store them in th `dl_path`, and deletes the Excel files if the `delete` flag is set

        - - -
        ### Parameters
        - `stage_path`: The stage path where Excel files are stored
        - `dl_path`: The path to the datalake where the csv files are stored
        - `delete`: Indicates whether to delete the Excel files or not

    '''
    if stage_path is None:
        stage_path = os.path.join(os.getcwd(), "Stage")
    else:
        try:
            validate_path(stage_path)
        except ValueError as ve:
            raise ValueError(f"Stage {str(ve)}")

    if dl_path is None:
        dl_path = os.path.join(os.getcwd(), "Datalake")
    else:
        try:
            validate_path(dl_path)
        except ValueError as ve:
            raise ValueError(f"Datalake {str(ve)}")

    make_dir(dl_path)

    excels = get_excel_files(stage_path)

    if len(excels) == 0:
        raise ValueError(
            f"There is no Excel file in the Stage directory.\nPath: {stage_path}")

    for excel in excels:

        if does_file_exists(os.path.join(dl_path, path_to_file_name(excel) + ".csv")):
            logger.info(
                f"The {path_to_file_name(excel)}.csv already exists in the datalake.")
            continue

        convert_to_csv(excel, dl_path, delete)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Converts the Excel data to csv files.")

    parser.add_argument("-sp", "--stage_path", type=str,
                        help="Path to the stage directory (Do not specify if the stage folder is in the current directory)")

    parser.add_argument('-dp', '--datalake_path', type=str,
                        help="The path to the datalake folder (do not specify if you want to create a datalake directory in the current path)")

    parser.add_argument("-d", "--delete", type=str, choices=[
                        'yes', 'no'], default='no',
                        help="Indicates whether to delete Excel files or not.")

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

    stage_path = args.stage_path
    dl_path = args.datalake_path
    delete = True if args.delete == 'yes' else False

    try:
        conversion_handler(stage_path, dl_path, delete)

    except ValueError as v:
        logger.critical(str(v))
        sys.exit(v.args[0])

    except Exception as e:
        logger.critical(f"Unkown exception occured. {str(e)}")
        sys.exit(0)
