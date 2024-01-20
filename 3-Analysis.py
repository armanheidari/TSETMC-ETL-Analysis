import os
import sys
import pandas as pd
from pathlib import Path
from glob import glob
from typing import Tuple, List, Optional
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


class MarketCSVAnalyzer:

    def __init__(self, dl_path: Optional[str] = None, analyze_num: Optional[int] = 10) -> None:
        '''
        Initializes the Analyzer

        - - -
        ### Parameters
        - `dl_path`: The path to the datalake where csv files are stored
        - `analyze_num`: The number of stocks to be included in the analysis results

        '''
        if dl_path is None:
            self.dl_path = os.path.join(os.getcwd(), "Datalake")
        else:
            try:
                validate_path(dl_path)
            except ValueError as ve:
                raise ValueError(f"Datalake {str(ve)}")

        self.files = self._get_csv_files()
        if len(self.files) == 0:
            raise ValueError(
                f"There is no CSV file in the data lake directory.\nPath: {dl_path}")

        self.analyze_num = 10 if analyze_num is None else analyze_num

        if self.analyze_num <= 0:
            raise ValueError(
                f"The analyze number ({self.analyze_num}) is not valid.\nIt must be larger than 0.")

        self.integrated_data = self._integrate_data()
        self.number_of_stocks = self.integrated_data["نماد"].unique().shape[0]

        if self.analyze_num > self.number_of_stocks:
            raise ValueError(
                f"The analyze number ({self.analyze_num}) is larger than the number of stocks ({self.number_of_stocks})!")

        self.summed_data = self._sum_data()
        self.fl_data = self._first_last_data()

    def _get_csv_files(self) -> List[str]:
        '''
        Retrieves all the csv files in the `self.dl_path`.

        ---
        ### Returns
        - `List[str]`: A list of all csv files in the data lake

        '''
        return glob(self.dl_path + "/*.csv")

    def _integrate_data(self) -> pd.DataFrame:
        '''
        Concatenates all the data stored in the data lake into a DataFrame.

        ---
        ### Returns
        - `pd.DataFrame`: All data concatenated together

        '''
        data = []

        for file in self.files:
            data.append(pd.read_csv(file))

        return pd.concat(data)

    def _sum_data(self) -> pd.DataFrame:
        '''
        Aggregates the number, volume, and value for each stock over time into a DataFrame.

        ---
        ### Returns
        - `pd.DataFrame`: A DataFrame containing the aggregated data over time for each stock

        '''
        summed_data = self.integrated_data[["نماد", "تعداد", "حجم", "ارزش"]]
        summed_data = summed_data.groupby("نماد").sum(
            numeric_only=True).reset_index()
        return summed_data

    def _first_last_data(self) -> pd.DataFrame:
        '''
        Finds the first and last price for each stock in the data, and calculates the percentage change/

        ---
        ### Returns
        - `pd.DataFrame`: A DataFrame containing the first price, last price, and the percentage change for each stock

        '''
        first_rows = self.integrated_data.groupby(
            'نماد').first().reset_index()
        last_rows = self.integrated_data.groupby(
            'نماد').last().reset_index()

        first_last_data = pd.DataFrame({
            "نماد": first_rows["نماد"],
            "اولین": first_rows["اولین"],
            "قیمت پایانی - مقدار": last_rows["قیمت پایانی - مقدار"]
        })

        first_last_data["درصد تغییر"] = (
            ((first_last_data["قیمت پایانی - مقدار"] - first_last_data["اولین"]) / first_last_data["اولین"]) * 100).round(2)

        return first_last_data

    def _max_min_turnover(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        '''
        Analyzes the `self.integrated_data` DataFrame and identifies the `self.analyze_num` stocks with the highest and lowest exchange volumes.

        ---
        ### Returns
        - `pd.DataFrame`: A DataFrame containing `self.analyze_num` stocks with the highest exchange volumes
        - `pd.DataFrame`: A DataFrame containing `self.analyze_num` stocks with the lowest exchange volumes

        '''
        return self.summed_data.nlargest(self.analyze_num, "حجم")[["نماد", "حجم"]].set_index("نماد"),  self.summed_data.nsmallest(self.analyze_num, "حجم")[["نماد", "حجم"]].set_index("نماد")

    def _max_min_number(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        '''
        Analyzes the `self.integrated_data` DataFrame and identifies the `self.analyze_num` stocks with the highest and lowest exchange number.

        ---
        ### Returns
        - `pd.DataFrame`: A DataFrame containing `self.analyze_num` stocks with the highest exchange number
        - `pd.DataFrame`: A DataFrame containing `self.analyze_num` stocks with the lowest exchange number

        '''
        return self.summed_data.nlargest(self.analyze_num, "تعداد")[["نماد", "تعداد"]].set_index("نماد"),  self.summed_data.nsmallest(self.analyze_num, "تعداد")[["نماد", "تعداد"]].set_index("نماد")

    def _max_min_value(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        '''
        Analyzes the `self.integrated_data` DataFrame and identifies the `self.analyze_num` stocks with the highest and lowest exchange value.

        ---
        ### Returns
        - `pd.DataFrame`: A DataFrame containing `self.analyze_num` stocks with the highest exchange value.
        - `pd.DataFrame`: A DataFrame containing `self.analyze_num` stocks with the lowest exchange value.

        '''
        return self.summed_data.nlargest(self.analyze_num, "ارزش")[["نماد", "ارزش"]].set_index("نماد"),  self.summed_data.nsmallest(self.analyze_num, "ارزش")[["نماد", "ارزش"]].set_index("نماد")

    def _max_increase(self) -> pd.DataFrame:
        '''
        Analyzes the `self.integrated_data` DataFrame and identifies the `self.analyze_num` stocks with the highest price increase.

        ---
        ### Returns
        - `pd.DataFrame`: A DataFrame containing `self.analyze_num` stocks with the highest price increase, the initial and final price, and the increase percentage

        '''
        return self.fl_data[self.fl_data["درصد تغییر"] > 0].nlargest(self.analyze_num, "درصد تغییر").set_index("نماد")

    def _max_decrease(self) -> pd.DataFrame:
        '''
        Analyzes the `self.integrated_data` DataFrame and identifies the `self.analyze_num` stocks with the highest price decrease.

        ---
        ### Returns
        - `pd.DataFrame`: A DataFrame containing `self.analyze_num` stocks with the highest price decrease, the initial and final price, and the decrease percentage

        '''
        return self.fl_data[self.fl_data["درصد تغییر"] < 0].nsmallest(self.analyze_num, "درصد تغییر").set_index("نماد")

    def analyze(self) -> None:
        '''
            Performs all the defined analysis on the data and makes the result.html file

        '''
        max_t, min_t = self._max_min_turnover()
        with open('result.html', 'w', encoding="UTF-8") as f:
            f.write(
                "<!DOCTYPE html>\n<html lang=\"en-US\">\n<head>\n<meta charset=\"UTF-8\" />\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />\n<link rel=\"shortcut icon\" href=\"logo.jpg\" type=\"image/x-icon\" />\n<title>Analysis Results</title>\n</head>\n<style>\n* {\nmargin: 0;\npadding: 0;\nbox-sizing: border-box;\n}\n\nhtml {\nfont-size: calc((100vw / 192) * 2.25);\n}\n\nbody {\nbackground-color: #777;\n}.container {\ndisplay: flex;\njustify-content: space-between;\nwidth: 100%;\npadding: 10px 5% 25px 5%;\n}\n.inner_cont {\nwidth: 45%;\nmargin: 10px 0;\npadding: 20px 0;\ntext-align: center;\nalign-items: center;\nbackground-color: rgba(27, 190, 208, 0.6);\nborder-radius: 20px;\n}\n.dataframe {\nmargin: 0 auto;\n}\nh1 {\nmargin-bottom: 15px;\n}\n</style>\n<body>\n")

            f.write('<div class="container">\n<div class="inner_cont">\n')
            f.write('<h1> Highest Exchange Turnover </h1>\n')
            f.write(max_t.to_html())
            f.write('\n</div>\n<div class="inner_cont">\n')
            f.write('<h1> Lowest Exchange Turnover </h1>\n')
            f.write(min_t.to_html())

            f.write("\n</div>\n</div>\n")

        max_n, min_n = self._max_min_number()
        with open('result.html', 'a', encoding="UTF-8") as f:

            f.write('<div class="container">\n<div class="inner_cont">\n')

            f.write('<h1> Highest Exchange Number </h1>\n')
            f.write(max_n.to_html())

            f.write('\n</div>\n<div class="inner_cont">\n')

            f.write('<h1> Lowest Exchange Number </h1> </h1>\n')
            f.write(min_n.to_html())

            f.write("\n</div>\n</div>\n")

        max_v, min_v = self._max_min_value()
        with open('result.html', 'a', encoding="UTF-8") as f:
            f.write('<div class="container">\n<div class="inner_cont">\n')

            f.write('<h1> Highest Exchange Value </h1>\n')
            f.write(max_v.to_html())

            f.write('\n</div>\n<div class="inner_cont">\n')

            f.write('<h1> Lowest Exchange Value </h1>\n')
            f.write(min_v.to_html())

            f.write("\n</div>\n</div>\n")

        max_i, max_d = self._max_increase(), self._max_decrease()
        with open('result.html', 'a', encoding="UTF-8") as f:
            f.write('<div class="container">\n<div class="inner_cont">\n')

            f.write('<h1> Highest Price Increase </h1>\n')
            f.write(max_i.to_html())

            f.write('\n</div>\n<div class="inner_cont">\n')

            f.write('<h1> Highest Price Decrease </h1>\n')
            f.write(max_d.to_html())

            f.write("\n</div>\n</div>")
            f.write("\n</body>\n</html>")

        logger.info(
            "Data Analyzed Successfuly.\n Check result.html for visual results.")

        exit_code = os.system(f"start result.html")  # - Widnows

        if exit_code != 0:
            exit_code = os.system(f"open result.html")  # - Mac os

            if exit_code != 0:
                exit_code = os.system(f"xdg-open result.html")  # - Linux

                if exit_code != 0:
                    logger.error("Can not open the result.html")


if __name__ == "__main__":

    parser = ArgumentParser(
        description="Analyze the csv data from the datalake.")

    parser.add_argument('-p', '--datalake_path', type=str,
                        help="The path to the data lake folder (do not specify if the data lake is in the current directory)")

    parser.add_argument('-a', '--analyze_number', type=int,
                        help="The number of stocks to be included in the analysis results)")

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

    dl_path = args.datalake_path
    analyze_num = args.analyze_number

    try:
        csv_analyzer = MarketCSVAnalyzer(dl_path, analyze_num)
        csv_analyzer.analyze()

    except ValueError as v:
        logger.critical(str(v))
        sys.exit(v.args[0])

    except Exception as e:
        logger.critical(f"Unkown exception occured. {str(e)}")
        sys.exit(0)
