<p align="center"><img src=https://github.com/stark4n6/ASP-Search/blob/main/assets/asp.png alt="ASP" width="300" height="300"></p>

# ASP (App Store Package) Search
ASP aka App Store Package Search is a script written in Python (why did it have to be snakes) that performs lookup actions of bundle IDs or Adam IDs against the Apple App Store and returns information about each app.

Much thanks to pug4N6 [for the original idea](https://github.com/pug4N6/bundleID_lookup) from which this drastically transformed out of.

## DISCLAIMER
The script works on Windows but may not have support on other OS's, feedback is greatly appreciated!

You may find timeout issues if too many searches are run, use at your own risk.

## Usage

### Requirements
`pip install -r requirements.txt`<p>
Only Pillow is needed (it's just for logo usage), it may be easier to just use the .exe anyhow.

### GUI Interface

1. Input can be any of the following:
   - A single BundleID
   - A single AdamID
   - A text file of BundleIDs (one per line)
   - A test file of AdamIDs (one per line)

2. Choose your lookup type accordingly
3. Choose your output type option
4. Choose your output folder path
5. Execute!

<p align="center"><img width="752" height="702" alt="Image" src="https://github.com/user-attachments/assets/59328e8a-67da-4718-b5f8-7acccf751774" /></p>
