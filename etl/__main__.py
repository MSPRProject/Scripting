from extract import Extractor
from load import Loader
from transform import Transformer
import sys
import os
import extractors

def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <path>")
        sys.exit(1)

    extractors = []
    transformer = Transformer()
    loader = Loader()
    base = sys.argv[1]

    for e in Extractor.__subclasses__():
        print(f"Found extractor: {e.__name__}")
        extractors.append(e())

    if os.path.isdir(base):
        for f in os.listdir(base):
            handle_file(extractors, transformer, loader, os.path.join(base, f))
    else:
        handle_file(extractors, transformer, loader, base)

def handle_file(extractors: [Extractor], transformer: Transformer, loader: Loader, path: str):
    for extractor in extractors:
        df = extractor.try_extract(path)
        if df is not False:
            print(df.info())
            print(df.sample(30))
            transformer.transform(df)
            loader.load(transformer)
            return
    raise Exception(f"Could not find extractor for {path}")

if __name__ == '__main__':
    main()
