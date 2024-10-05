import requests
import os


def extract(
    url="https://raw.githubusercontent.com/acgowda/cfb-recruit-net/525eea9f7a803080e57cee3e8b0cc0dd319ce0d3/data/2020/usc_offers.csv",
    file_path="data/usc_offers.csv",
):
    """ "Extract a url to a file path"""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # Fetch the content from the URL
    response = requests.get(url)

    # Check for valid response status
    if response.status_code == 200:
        # Save the content to the specified file path
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"File successfully downloaded to {file_path}")
    else:
        print(f"Failed to retrieve the file. HTTP Status Code: {response.status_code}")

    return file_path
