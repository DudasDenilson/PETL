import pandas as pd


def import_csv(local):
    """
    Perform CSV import , receive at parameter a location where the file is, can be a physical
    machine location or a URL and return a panda dataframe.
    """

    data = None
    try:
        print('Starting CSV import')
        data = pd.read_csv(local, sep=",")
        print('CSV import is done')
    except Exception as e:
        print('Problem to open CSV ' + str(e))

    return data
