from man.notebooker.handle_overrides import _handle_overrides

if __name__ == '__main__':
    print _handle_overrides('import ahl.marketdata as amd\na=amd.describe("FTL")')
