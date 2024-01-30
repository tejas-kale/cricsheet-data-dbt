import os
import shutil
from io import BytesIO
from typing import List, Any, Dict, Optional
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

MATCH_INFO_SCHEMA = [
    {
        "name": "row_type",
        "type": "STRING"
    },
    {
        "name": "info_category",
        "type": "STRING"
    },
    {
        "name": "info_value",
        "type": "STRING"
    },
    {
        "name": "info_value_2",
        "type": "STRING"
    },
    {
        "name": "info_value_3",
        "type": "STRING"
    },
    {
        "name": "match_id",
        "type": "STRING"
    }
]
BALL_DATA_SCHEMA = [
    {
        "name": "match_id",
        "type": "STRING"
    },
    {
        "name": "season",
        "type": "STRING"
    },
    {
        "name": "start_date",
        "type": "DATE"
    },
    {
        "name": "venue",
        "type": "STRING"
    },
    {
        "name": "innings",
        "type": "INTEGER"
    },
    {
        "name": "ball",
        "type": "FLOAT"
    },
    {
        "name": "batting_team",
        "type": "STRING"
    },
    {
        "name": "striker",
        "type": "STRING"
    },
    {
        "name": "non_striker",
        "type": "STRING"
    },
    {
        "name": "bowler",
        "type": "STRING"
    },
    {
        "name": "runs_off_bat",
        "type": "INTEGER"
    },
    {
        "name": "extras",
        "type": "INTEGER"
    },
    {
        "name": "wides",
        "type": "INTEGER"
    },
    {
        "name": "noballs",
        "type": "INTEGER"
    },
    {
        "name": "byes",
        "type": "INTEGER"
    },
    {
        "name": "legbyes",
        "type": "INTEGER"
    },
    {
        "name": "penalty",
        "type": "INTEGER"
    },
    {
        "name": "wicket_type",
        "type": "STRING"
    },
    {
        "name": "player_dismissed",
        "type": "STRING"
    },
    {
        "name": "other_wicket_type",
        "type": "STRING"
    },
    {
        "name": "other_player_dismissed",
        "type": "STRING"
    }
]


class CricsheetDataIngestor:
    url: str = "https://cricsheet.org/downloads/all_csv2.zip"
    temp_dir: str = "/tmp/cricsheet_data"

    def __init__(
        self,
        bq_project_id: str,
        bq_dataset_name: str,
        match_info_table_name: str,
        ball_data_table_name: str,
    ):
        self.bq_project_id = bq_project_id
        self.bq_dataset_name = bq_dataset_name
        self.match_info_table_name = match_info_table_name
        self.ball_data_table_name = ball_data_table_name

    def ingest(self):
        """
        Ingest new cricket match data into BigQuery database. This method
        downloads all available data from Cricsheet, filters out data that
        is already in the database, and then ingests the remaining data.
        """
        # self.download_data()

        # Get the list of match IDs to process.
        downloaded_csv_files = self.get_csv_files()
        existing_match_ids: List[str] = self.get_existing_match_ids()
        match_ids_to_process: List[str] = self.get_match_ids_to_process(
            downloaded_csv_files, existing_match_ids
        )
        count_of_new_matches: int = len(match_ids_to_process)
        print(f"Count of match IDs to process: {count_of_new_matches}")

        # Ingest data for new matches.
        batch_info_dfs: List[pd.DataFrame] = []
        batch_ball_dfs: List[pd.DataFrame] = []
        num_files_processed: int = 0
        batch_size: int = 500
        for match_id in match_ids_to_process:
            batch_info_dfs.append(self.load_info_csv(match_id))
            batch_ball_dfs.append(self.load_ball_csv(match_id))

            current_batch_size: int = len(batch_info_dfs)
            if ((current_batch_size == batch_size) or
                ((current_batch_size + num_files_processed)
                 == count_of_new_matches)):
                num_files_processed += batch_size
                print(f"Processed {num_files_processed} files.")
                self.save_data_to_gbq(
                    pd.concat(batch_info_dfs),
                    self.match_info_table_name,
                    MATCH_INFO_SCHEMA
                )
                self.save_data_to_gbq(
                    pd.concat(batch_ball_dfs),
                    self.ball_data_table_name,
                    BALL_DATA_SCHEMA
                )
                batch_info_dfs.clear()
                batch_ball_dfs.clear()

        # Delete the temporary directory.
        self.delete_temp_dir()

    def download_data(self):
        """
        Download the data from the URL and extract it to the temp directory.
        """
        with urlopen(self.url) as zip_file:
            with ZipFile(BytesIO(zip_file.read())) as zfile:
                zfile.extractall(self.temp_dir)

    def get_existing_match_ids(self) -> List[str]:
        """
        Get all the match IDs from the database.

        To avoid issues during first execution, a try-except block is used
        to check if the table exists. If it does not, an empty list is
        returned.
        """
        # Check if the table exists.
        bq_client = bigquery.Client(project=self.bq_project_id)
        try:
            bq_client.get_table(
                f"{self.bq_project_id}.{self.bq_dataset_name}."
                f"{self.match_info_table_name}"
            )
        except NotFound:
            return []

        # Get the list of match IDs.
        q: str = f"""
        select distinct match_id as match_id
        from {self.bq_dataset_name}.{self.match_info_table_name}
        """
        return pd.read_gbq(q, self.bq_project_id).match_id.tolist()

    def get_csv_files(self) -> List[str]:
        """
        Get all the CSV files from the temporary directory.
        """
        return [x for x in os.listdir(self.temp_dir)
                if "_info" not in x and x.endswith(".csv")]

    def get_match_ids_to_process(
        self,
        downloaded_csv_files: List[str],
        existing_match_ids: List[str]
    ) -> List[str]:
        """
        Get the list of match IDs to process.

        This method first gets the match ID for each CSV file using the
        `get_match_id()` method. It then takes a set difference between the
        prospective match IDs and the existing match IDs, and returns the
        difference as a list.

        :param downloaded_csv_files: List of CSV files.
        :param existing_match_ids: List of existing match IDs.
        :return: List of match IDs to process.
        """
        prospective_match_ids = [self.get_match_id(csv_file)
                                 for csv_file in downloaded_csv_files]
        return list(set(prospective_match_ids) - set(existing_match_ids))

    @staticmethod
    def get_match_id(csv_file_name: str) -> str:
        """
        Get match ID from CSV file name.

        :param csv_file_name: Name of the CSV file.
        :return: Match ID as string.
        """
        return csv_file_name.split(".")[0]

    def load_info_csv(self, match_id: str) -> pd.DataFrame:
        """
        Load the info CSV file for the input match ID.

        There are matches for which the info CSV file is missing. In such cases,
        a message with the match ID is printed and an empty DataFrame is
        returned. Also, a column `match_id` is added to the DataFrame.

        :param match_id: Match ID as string.
        :return: Pandas DataFrame containing the info CSV data.
        """
        match_info_fp: str = self.get_valid_csv_fp(match_id, True)
        if not match_info_fp:
            return pd.DataFrame([])

        info_df: pd.DataFrame = pd.read_csv(
            match_info_fp,
            names=[
                "row_type",
                "info_category",
                "info_value",
                "info_value_2",
                "info_value_3"
            ]
        )
        info_df.loc[:, "match_id"] = match_id
        return info_df

    def get_valid_csv_fp(
        self,
        match_id: str,
        is_info_file: bool = False
    ) -> Optional[str]:
        """
        Get the valid CSV file path for the input match ID.

        :param match_id: Match ID as integer.
        :param is_info_file: Boolean indicating if the file is an info file.
        :return: Valid CSV file path as string or None.
        """
        if is_info_file:
            csv_fp: str = os.path.join(
                self.temp_dir,
                f"{match_id}_info.csv"
            )
        else:
            csv_fp: str = os.path.join(self.temp_dir, f"{match_id}.csv")

        if os.path.exists(csv_fp):
            return csv_fp
        else:
            info_str: str = "info" if is_info_file else ""
            print(f"Match ID {match_id} does not have {info_str} CSV file.")
            return None

    def load_ball_csv(self, match_id: str) -> pd.DataFrame:
        """
        Load the ball CSV file for the input match ID.

        The columns `other_wicket_type` and `other_player_dismissed` are often
        missing which gets interpreted as floats when saving to BigQuery (which
        in turn relies on Parquet interpreting them as floats). To avoid this,
        we replace the missing values with empty strings as the columns are
        meant to hold string values when available.

        We cast the `season` column as string as it also contains integer
        values. Also, we cast the `match_id` column to string as it can contain
        IDs like `wi_*`.

        :param match_id: Match ID as string.
        :return: Pandas DataFrame containing the ball CSV data.
        """
        match_ball_fp: str = self.get_valid_csv_fp(match_id)
        if not match_ball_fp:
            return pd.DataFrame([])

        ball_df: pd.DataFrame = pd.read_csv(match_ball_fp)

        ball_df = ball_df.fillna(value={
            "other_wicket_type": "",
            "other_player_dismissed": ""
        })
        ball_df.loc[:, "season"] = ball_df["season"].astype(str)
        ball_df.loc[:, "match_id"] = ball_df["match_id"].astype(str)
        return ball_df

    def save_data_to_gbq(
        self,
        df: pd.DataFrame,
        table_name: str,
        table_schema: List[Dict[str, Any]]
    ):
        """
        Append the data to BigQuery table.

        :param df: Pandas DataFrame containing the data to append.
        :param table_name: Name of the BigQuery table.
        :param table_schema: Schema of the BigQuery table.
        """
        df.to_gbq(
            f"{self.bq_dataset_name}.{table_name}",
            self.bq_project_id,
            table_schema=table_schema,
            if_exists="append"
        )

    def delete_temp_dir(self):
        """
        Delete the temporary directory.
        """
        # shutil.rmtree(self.temp_dir)
        pass


if __name__ == "__main__":
    cdi = CricsheetDataIngestor(
        bq_project_id="august-cirrus-399913",
        bq_dataset_name="cricsheet_raw",
        match_info_table_name="match_info",
        ball_data_table_name="ball_data"
    )
    cdi.ingest()
