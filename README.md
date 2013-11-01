# mortar-luigi

## Installation

    # create a virtualenv
    virtualenv ~/code/ve_luigi
    source ~/code/ve_luigi/bin/activate

    # install mortar's fork of luigi
    cd ~/code
    git clone git@github.com:mortardata/luigi.git
    cd luigi
    python setup.py develop
    
    # install mortar-api-python
    cd ~/code
    git clone git@github.com:mortardata/mortar-api-python.git
    cd mortar-api-python
    python setup.py develop
    
    # install mortar-luigi
    cd ~/code
    git clone git@github.com:mortardata/mortar-luigi.git
    cd mortar-luigi
    python setup.py develop

## 'Sample-Test' Mode

* Uses the --sample-test command line argument
* Data will be assumed to be in s3://<bucket_name>/sample_data/input/
* Failed validations of dynamodb tables or API will be ignored
* Local client.cfg should be set to non-production dynamodb parameters


