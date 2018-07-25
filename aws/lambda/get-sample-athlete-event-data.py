import json
import urllib.parse

sample = {
          "CompositeKey": "45178|JPN|2004|Wrestling|Wrestling Women's Heavyweight, Freestyle",
          "ID": "45178",
          "Name": "Kyoko Hamaguchi",
          "Sex": "F",
          "Age": "26",
          "Height": "170",
          "Weight": "72",
          "Team": "Japan",
          "NOC": "JPN",
          "Games": "2004 Summer",
          "Year": "2004",
          "Season": "Summer",
          "City": "Athina",
          "Sport": "Wrestling",
          "Event": "Wrestling Women's Heavyweight, Freestyle",
          "Medal": "Bronze"
         }


def lambda_handler(event, context):
    return sample