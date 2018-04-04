# intake-mongo

Intake-Mongo: Mongodb Plugin for Intake

## User Installation

*Note: the following command does not work yet, and the developer installation is recommended.*
```
conda install -c intake intake-mongo
```

## Developer Installation

1. Create a development environment with `conda create`. Then install the dependencies:

    ```
    conda install -c intake intake
    conda install -n root conda-build
    git clone https://github.com/ContinuumIO/MongoAdapter.git
    conda build MongoAdapter/buildscripts/condarecipe
    conda install --use-local mongoadapter
    ```

1. Development installation:
    ```
    python setup.py develop --no-deps
    ```

1. Running tests requires pytest, run them using pytest from the base directory:
   ```
   conda install pytest
   ```
