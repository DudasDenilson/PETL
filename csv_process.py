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


def write_csv(lista_dict_origem, caminho_csv_destino):
    """
    Perform CSV write using a dictionary list receive at parameter on specific local.
    """
    sucess = False
    try:
        print('Starting CSV write')
        # Get keys at parameter list
        colunas = lista_dict_origem[0].keys()
        array_colunas = []

        # Create array with all columns
        for r in colunas:
            array_colunas.append(r)

        # Create dataframe based on columns above
        df = pd.DataFrame(lista_dict_origem, columns=array_colunas)

        # Sort CSV data using columns 0 and 1
        df = df.sort_values(by=[array_colunas[0], array_colunas[1]])

        # Create CSV
        df.to_csv(caminho_csv_destino, index=None, header=True)
        sucess = True

        print('CSV write process is done')
    except Exception as e:
        print('Failed to write CSV ' + str(e))

    return sucess
