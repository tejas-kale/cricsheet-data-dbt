import json
import os
from typing import List, Any, Dict, Tuple

import sqlite3


class CricsheetDataIngestor:
    def __init__(self, data_dir: str, json_files_dir: str, db_path: str):
        self.data_dir = data_dir
        self.json_files_dir = json_files_dir
        db_conn = sqlite3.connect(db_path)
        self.db_cursor = db_conn.cursor()

    def ingest(self):
        """

        """
        # Get the list of JSON files to process.
        json_files = self.get_json_files()
        existing_match_ids: List[int] = self.get_existing_match_ids()
        json_files_to_process = self.get_json_files_to_process(
            json_files, existing_match_ids
        )
        for json_file in json_files_to_process:
            match_id: int = self.get_match_id(json_file)
            match_data: Dict[str, Any] = self.load_json(json_file)
            match_info: Dict[str, Any] = match_data["info"]
            venue_id: int = self.save_and_get_venue_id(match_info)
            match_type_id: int = self.save_and_get_match_type_id(match_info)

    def get_json_files(self) -> List[str]:
        """
        Get all the JSON files from the JSON files directory.
        """
        return os.listdir(self.json_files_dir)

    def get_existing_match_ids(self) -> List[int]:
        """
        Get all the match IDs from the database.
        """
        self.db_cursor("SELECT DISTINCT match_id FROM match")
        rows = self.db_cursor.fetchall()
        return [row[0] for row in rows]

    def get_json_files_to_process(
        self,
        json_files: List[str],
        existing_match_ids: List[int]
    ) -> List[str]:
        """
        Get the list of JSON files to process.

        This method first gets the match ID for each JSON file using the `get_match_id()` method.
        It then takes a set difference between the prospective match IDs and the existing match IDs,
        and returns the difference as a list.

        :param json_files: List of JSON files.
        :param existing_match_ids: List of existing match IDs.
        :return: List of JSON files to process.
        """
        prospective_match_ids = [self.get_match_id(json_file)
                                 for json_file in json_files]
        return [f"{match_id}.json"
                for match_id in
                list(set(prospective_match_ids) - set(existing_match_ids))]

    @staticmethod
    def get_match_id(json_file_name: str) -> int:
        """
        Get match ID from JSON file name.

        :param json_file_name: Name of the JSON file.
        :return: Match ID as integer.
        """
        return int(json_file_name.split(".")[0])

    def load_json(self, json_file_name: str) -> Dict[str, Any]:
        """
        Load a JSON file and return it as a dictionary.

        :param json_file_name: Name to the JSON file.
        :return: The loaded JSON as a dictionary.
        """
        with open(
            os.path.join(self.data_dir, self.json_files_dir, json_file_name),
            "r"
        ) as json_file:
            data: Dict[str, Any] = json.load(json_file)
        return data

    def execute_insert_query(
        self,
        table_name: str,
        param_names: Tuple[str, str],
        param_values: Tuple[str, str]
    ) -> None:
        """
        Execute a query that inserts records.

        :param table_name: Name of the table to insert into.
        :param param_names: Names of the parameters for the query.
        :param param_values: Values of the parameters for the query.
        """
        self.db_cursor.execute(
            f"INSERT OR IGNORE INTO {table_name} {param_names} VALUES (?, ?)",
            param_values
        )

    def save_and_get_venue_id(self, match_info: Dict[str, Any]) -> int:
        """
        Save venue information and get the venue ID.

        This method first gets the venue and city information from the match_info dictionary.
        It then inserts the venue and city information into the venue table in the database.
        If the venue and city already exist in the table, any error raised is silently ignored.
        Finally, it queries the venue table for the venue and city and returns the venue ID.

        :param match_info: Dictionary containing match information.
        :return: Venue ID as integer.
        """
        venue = match_info.get("venue")
        city = match_info.get("city")

        self.execute_insert_query(
            "venue",
            ("venue", "city"),
            (venue, city)
        )

        self.db_cursor.execute(
            "SELECT venue_id FROM venue WHERE venue_name = ? AND city = ?",
            (venue, city)
        )
        venue_id = self.db_cursor.fetchone()[0]
        return venue_id

    def save_and_get_match_type_id(self, match_info: Dict[str, Any]) -> int:
        """
        Save match type information and get the type ID.

        This method first gets the match type from the match_info dictionary.
        It then inserts the match type into the match_type table in the database.
        If the match type already exists in the table, any error raised is silently ignored.
        Finally, it queries the match_type table for the match type and returns the type ID.

        :param match_info: Dictionary containing match information.
        :return: Type ID as integer.
        """
        match_type = match_info.get("match_type")

        self.execute_insert_query(
            "match_type",
            ("name",),
            (match_type,)
        )

        self.db_cursor.execute(
            "SELECT type_id FROM match_type WHERE name = ?",
            (match_type,)
        )
        type_id = self.db_cursor.fetchone()[0]
        return type_id


if __name__ == "__main__":
    db_loc: str = "/Users/tejaskale/Code/cricsheet_data/data/cricsheet.sqlite"
