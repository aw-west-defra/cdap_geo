from collections import defaultdict
from glob import glob
from itertools import chain, product
from os.path import basename
from typing import Any, DefaultDict, Iterator, List, Tuple
from xml.etree.ElementTree import (  # noqa: S405 - N/A to user-created XML
    Element,
    ElementTree,
    SubElement,
)

from osgeo.ogr import Open


def _list_layers(path: str) -> Tuple[Any, ...]:
    """Given a path to an OGR file, returns a list of layers.

    Example:
       >>> list_layers("/path/to/ogr/file")
       ["layer0", "layer1", "layer2"]

    Args:
        path (str): path to an OGR file.

    Returns:
        List[str]: a list of layers.
    """
    data_source = Open(path)
    return tuple(
        data_source.GetLayer(index).GetName()
        for index in range(data_source.GetLayerCount())
    )


def _create_path_layer_tuples(paths: List[str]) -> Iterator[Tuple[str, str]]:
    return chain.from_iterable([product([path], _list_layers(path)) for path in paths])


def _create_paths_by_layer_dict(
    path_layer_tuples: Iterator[Tuple[str, str]]
) -> DefaultDict[str, List[str]]:
    paths_by_layer = defaultdict(list)
    for path, layer in path_layer_tuples:
        paths_by_layer[layer].append(path)
    return paths_by_layer


def _create_vrt_xml(paths_by_layer: DefaultDict[str, List[str]]) -> ElementTree:
    data_source = Element("OGRVRTDataSource")

    for layer, paths in paths_by_layer.items():
        union_layer = SubElement(data_source, "OGRVRTUnionLayer", {"name": layer})
        layers = [
            SubElement(
                union_layer, "OGRVRTLayer", {"name": basename(path).split(".")[0]}
            )
            for path in paths
        ]

        source_elements = [SubElement(layer, "SrcDataSource") for layer in layers]

        for index, source_element in enumerate(source_elements):
            source_element.text = paths[index]

        layer_elements = [SubElement(layer, "SrcLayer") for layer in layers]

        for layer_element in layer_elements:
            layer_element.text = layer

    return ElementTree(element=data_source)


def _vrt_from_vector_files(path: str) -> ElementTree:
    """# TODO: public function docstring."""
    paths = glob(path)
    path_layer_tuples = _create_path_layer_tuples(paths)
    paths_by_layer_dict = _create_paths_by_layer_dict(path_layer_tuples)
    return _create_vrt_xml(paths_by_layer_dict)
