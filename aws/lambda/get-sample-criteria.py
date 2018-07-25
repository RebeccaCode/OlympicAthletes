import json
import urllib.parse

sample = {
          "criteria": [
            {
              "attribute": "Sport",
              "operator": "Equals",
              "value": "Wrestling"
            },
            {
              "attribute": "Medal",
              "operator": "Does not equal",
              "value": "NA"
            },
            {
              "attribute": "Sex",
              "operator": "Equals",
              "value": "F"
            },
            {
              "attribute": "NOC",
              "operator": "Equals",
              "value": "JPN"
            },
            {
              "attribute": "Year",
              "operator": "Greater than",
              "value": "2002"
            },
            {
              "attribute": "Year",
              "operator": "Less than",
              "value": "2013"
            }
          ]
        }


def lambda_handler(event, context):
    return sample