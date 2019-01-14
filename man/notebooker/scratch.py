from man.notebooker.handle_overrides import handle_overrides

if __name__ == '__main__':
    print handle_overrides('import ahl.marketdata as amd\na=amd.describe("FTL")')
