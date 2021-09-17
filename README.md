# ESA Geospatial Utilities

## Install

```{sh}
python -m pip install git+https://github.com/Defra-Data-Science-Centre-of-Excellence/esa_geo_utils
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

### Install GDAL 3.0.4

Add the [UbuntuGIS unstable Private Package Archive (PPA)](https://launchpad.net/~ubuntugis/+archive/ubuntu/ubuntugis-unstable)
and update your package list:

```{sh}
sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable \
    && sudo apt-get update
```

Install `gdal 3.0.4`, I found I also had to install python3-gdal (even though
I'm going to use poetry to install it in a virtual environment later) to
avoid version conflicts:

```{sh}
sudo apt-get install -y gdal-bin=3.0.4+dfsg-1build3 \
    libgdal-dev=3.0.4+dfsg-1build3 \
    python3-gdal=3.0.4+dfsg-1build3 
```
  
Verify the installation:

```{sh}
ogrinfo --version
# GDAL 3.0.4, released 2020/01/28
```

Add the Python bindings to your project:

```{sh}
poetry add GDAL==3.0.4
```
