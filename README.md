# ESA Geospatial Utilities

## Install

```{sh}
python -m pip install git+https//:
```

## Local development

To ensure compatibility with [Databricks Runtime 7.3 LTS](https://docs.databricks.com/release-notes/runtime/7.3.html), this package uses `Python 3.7.5`, `GDAL 2.4.2`, and `spark 3.0.1`.

### Install Python 3.7.5 using [pyenv](https://github.com/pyenv/pyenv)

Install `pyenv` using [`pyenv-installer`](https://github.com/pyenv/pyenv-installer):

```{sh}
curl https://pyenv.run | bash
```

Add the following lines to your `~/.bashrc`:

```{sh}
export PATH="~/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
```

Restart your shell so the path changes take effect:

```{sh}
exec $SHELL
```

Install Python 3.7.5:

```{sh}
pyenv install 3.7.5
```

### Install GDAL 2.4.2

Download the source file to your `Downloads` folder:

```{sh}
wget https://download.osgeo.org/gdal/2.4.2/gdal-2.4.2.tar.gz -O ~/Downloads/gdal-2.4.2.tar.gz
```

Extract to a folder in your home directory and `cd` into it:

```{sh}
tar -vxf ~/Downloads/gdal-2.4.2.tar.gz -C $HOME && cd ~/gdal-2.4.2
```

Build with Python bindings, where `/path/to/python` is the path to the python binary in your virtual environment of choice:

```{sh}
./configure --with-python=$(which python)
```

Complete installation:

```{sh}
make && sudo make install
```

Update the shared library cache:

```{sh}
sudo ldconfig
```

Verify the installation:

```{sh}
gdalinfo --version
# GDAL 2.4.2, released 2019/06/28
```

Add the Python bindings to your dependencies:

```{sh}
poetry add GDAL==2.4.2
```
