import importlib
import pkgutil

# Import all extractors so that they can be found with `Extractor.__subclasses__()`
for loader, module_name, is_pkg in pkgutil.walk_packages(__path__, __name__ + '.'):
    importlib.import_module(module_name)
