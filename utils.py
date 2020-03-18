import json
import os

def load_data(name="tinyTwitter.json"):
    print("Parsing Json file")
    with open(name, 'r') as f:
        # data = f.read().splitlines()
        # data = "".join(data)
        # json_data = json.loads(data[:-1]+"]}")
        data = f.read().strip()

        # The json bracket has not been closed
        if data[-1] == ',':
            print("Closing the file")
            data = data[:-1] + "]}"
        
        return json.loads(data)

if __name__ == "__main__":
    print(load_data()['total_rows'])