from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER, FLOAT, BOOLEAN


class fieldProcessing:
    """
    Class for parsing scraped dataframes and defining postgres datatypes before loading to database
    """

    # Parsing dataframes for corresponding fields and postgres datatypes
    def dataTypeParser(self, endpoint: str, fields):
        fieldsProcessed = [k for k, v in fields.items() if endpoint in v["type"]]
        dataTypes = [v["dataType"] for k, v in fields.items() if endpoint in v["type"]]

        return dict(zip(fieldsProcessed, dataTypes))

    def __init__(self, endpoint):
        # User-defined endpoint for field processing
        self.endpoint = endpoint

        # List of all fantasyPros-related basketball endpoints that are available
        self.allFantasyBaseballFields = [
            "firstBase",
            "secondBase",
            "thirdBase",
            "shortStop",
            "catcher",
            "outfield",
            "startPitcher",
            "reliefPitcher",
            "designatedHit",
        ]

        self.hitters = [
            "firstBase",
            "secondBase",
            "thirdBase",
            "shortStop",
            "catcher",
            "outfield",
            "designatedHit",
        ]

        self.pitchers = [
            "startPitcher",
            "reliefPitcher",
        ]

        # List of all fields available in scraped dataframes
        self.fields = {
            # All Players
            "VBR": {"type": self.allFantasyBaseballFields, "dataType": INTEGER()},
            "Player": {"type": self.allFantasyBaseballFields, "dataType": VARCHAR(200)},
            "Team": {"type": self.allFantasyBaseballFields, "dataType": VARCHAR(10)},
            "ROST%": {"type": self.allFantasyBaseballFields, "dataType": FLOAT()},
            "H": {"type": self.allFantasyBaseballFields, "dataType": FLOAT()},
            "BB": {"type": self.allFantasyBaseballFields, "dataType": FLOAT()},
            "HR": {"type": self.allFantasyBaseballFields, "dataType": FLOAT()},
            # Hitters Only
            "AB": {"type": self.hitters, "dataType": FLOAT()},
            "R": {"type": self.hitters, "dataType": FLOAT()},
            "RBI": {"type": self.hitters, "dataType": FLOAT()},
            "SB": {"type": self.hitters, "dataType": FLOAT()},
            "AVG": {"type": self.hitters, "dataType": FLOAT()},
            "OBP": {"type": self.hitters, "dataType": FLOAT()},
            "2B": {"type": self.hitters, "dataType": FLOAT()},
            "3B": {"type": self.hitters, "dataType": FLOAT()},
            "SO": {"type": self.hitters, "dataType": FLOAT()},
            "SLG": {"type": self.hitters, "dataType": FLOAT()},
            "OPS": {"type": self.hitters, "dataType": FLOAT()},
            # Pitchers Only
            "IP": {"type": self.pitchers, "dataType": FLOAT()},
            "K": {"type": self.pitchers, "dataType": FLOAT()},
            "W": {"type": self.pitchers, "dataType": FLOAT()},
            "SV": {"type": self.pitchers, "dataType": FLOAT()},
            "ERA": {"type": self.pitchers, "dataType": FLOAT()},
            "WHIP": {"type": self.pitchers, "dataType": FLOAT()},
            "ER": {"type": self.pitchers, "dataType": FLOAT()},
            "G": {"type": self.pitchers, "dataType": FLOAT()},
            "GS": {"type": self.pitchers, "dataType": FLOAT()},
            "L": {"type": self.pitchers, "dataType": FLOAT()},
            "CG": {"type": self.pitchers, "dataType": FLOAT()},
        }

        # Running datatype parser function on endpoint of interest
        self.dTypes = self.dataTypeParser(endpoint=self.endpoint, fields=self.fields)
