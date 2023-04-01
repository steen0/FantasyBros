import pandas as pd
import asyncio
import requests
import json
from unidecode import unidecode
from app.fantasyDataIngestion.databaseSetup import createEngine
from app.fantasyDataIngestion.dataTypeControllers import fieldProcessing
from datetime import datetime

# from espn_api.basketball import League


class webScraper:
    """
    Class for scraping fantasyPros projections from webpages and sending to postgres database
    """

    def __init__(self):

        self.engine = createEngine()

    async def getBaseballProjections(self, pos: str):

        fProsUrl = f"https://www.fantasypros.com/mlb/projections/ros-{pos}.php"

        try:
            playersDf = pd.read_html(fProsUrl)[0]
            playersDf["Team"] = (
                playersDf["Player"].str.split("\(").str[1].str.split(" \-").str[0]
            )
            playersDf["Player"] = playersDf["Player"].str.split(" \(").str[0]
            playersDf = playersDf[playersDf["VBR"].notnull()]
            playersDf = playersDf.drop(columns=["Unnamed: 17", "Unnamed: 18"], axis=1)

            return playersDf

        except Exception as e:
            print("Webscraping error:", str(e))

    async def sendDataToPostgres(self, df, table_name: str):

        engine = self.engine
        conn = engine.connect()

        metaData = fieldProcessing(endpoint=table_name)

        tableExistCheck = conn.execute(
            f"""SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE  table_schema = 'staging'
                AND    table_name   = '{table_name}'
                );"""
        ).fetchone()[0]

        if tableExistCheck:
            conn.execute(f'TRUNCATE TABLE "staging"."{table_name}"')

        print(f"Sending {table_name} data to postgres")

        try:
            df.to_sql(
                name=table_name,
                con=engine,
                schema="staging",
                if_exists="append",
                method="multi",
                dtype=metaData.dTypes,
            )

            print(f"Successfully sent {table_name} data to postgres!")

        except Exception as e:
            print("Could not send data to postgres: ", str(e))

    def asyncEtl(self):
        initTime = datetime.now()

        async def main(self):

            firstBase = await self.getBaseballProjections("1b")
            secondBase = await self.getBaseballProjections("2b")
            thirdBase = await self.getBaseballProjections("3b")
            shortStop = await self.getBaseballProjections("ss")
            catcher = await self.getBaseballProjections("c")
            outfield = await self.getBaseballProjections("of")
            startPitcher = await self.getBaseballProjections("sp")
            reliefPitcher = await self.getBaseballProjections("rp")
            designatedHit = await self.getBaseballProjections("dh")

            await asyncio.gather(
                self.sendDataToPostgres(df=firstBase, table_name="firstBase"),
                self.sendDataToPostgres(df=secondBase, table_name="secondBase"),
                self.sendDataToPostgres(df=thirdBase, table_name="thirdBase"),
                self.sendDataToPostgres(df=shortStop, table_name="shortStop"),
                self.sendDataToPostgres(df=catcher, table_name="catcher"),
                self.sendDataToPostgres(df=outfield, table_name="outfield"),
                self.sendDataToPostgres(df=startPitcher, table_name="startPitcher"),
                self.sendDataToPostgres(df=reliefPitcher, table_name="reliefPitcher"),
                self.sendDataToPostgres(df=designatedHit, table_name="designatedHit"),
            )

        asyncio.run(main(self))

        finTime = datetime.now()
        print("ETL Execution time: ", finTime - initTime)


if __name__ == "__main__":
    scraper = webScraper()
    scraper.asyncEtl()
