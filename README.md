# Sanalyz scripts

Tooling for the Sanalyz project

## ETL

An ETL pipeline for data extraction, transformation, and loading in the Sanalyz API.

### Prerequisites
- Python 3.8 or higher
- Required Python libraries:
  - `pandas`
  - `numpy`
  - `tqdm`
  - `requests`

Install the dependencies using:
```bash
pip install -r requirements.txt
```

### Usage

First download supported datasets in a folder. Currently, the following datasets are supported:
- [Monkey Pox](https://www.kaggle.com/datasets/utkarshx27/mpox-monkeypox-data/data)
- [Covid-19](https://www.kaggle.com/datasets/imdevskp/corona-virus-report?select=covid_19_clean_complete.csv)

Ensure that the datasets are in CSV format and placed in a folder. The folder structure should look like this:
```
data/
├── monkeypox.csv
├── covid19.csv
```

The name of the files are not important, as long as they are in CSV format.

If you want to add support for new datasets, see the [Adding Support for New Datasets](#adding-support-for-new-datasets) section.

When your dataset is ready, run the ETL pipeline with the following command:
```bash
python etl <path_to_datasets_folder> <api_base_url>
```

For example:
```bash
python etl data https://api.sanalyz.com
```

This will extract data from the datasets, clean it, transform it to be ready to be loaded, and then load it into the Sanalyz API.

### Adding Support for New Datasets

To add support for a new dataset:
1. Create a new extractor script in the `etl/extractors` folder.
2. Inherit from the `Extractor` base class and implement the `try_extract` method.
3. Refer to existing extractors (e.g., `covid.py`, `mpox.py`) for examples.

Once the new extractor is added, the ETL pipeline will automatically detect and use it.
