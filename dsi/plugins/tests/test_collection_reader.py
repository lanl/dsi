from dsi.core import Terminal
from collections import OrderedDict

from dsi.plugins.collection_reader import Dict

def test_ordered_dict_reader():
    my_dict = OrderedDict({'"2"': OrderedDict({'specification': ['!jack'], 'a': [1], 'b': [2], 'c': [45.98], 'd': [2], 'e': [34.8], 'f': [0.0089]}), 
                    'all': OrderedDict({'specification': ['!sam'], 'fileLoc': ['/home/sam/lib/data'], 'G': ['good memories'], 
                                        'all': [9.8], 'i': [2], 'j': [3], 'k': [4], 'l': [1.0], 'm': [99]}), 
                    'physics': OrderedDict({'specification': ['!amy', '!amy1'], 'n': [9.8, 91.8], 'o': ['gravity', 'gravity'], 
                                            'p': [23, 233], 'q': ['home 23', 'home 23'], 'r': [1, 12], 's': [-0.0012, -0.0122]}), 
                    'math': OrderedDict({'specification': [None, '!jack1'], 'a': [None, 2], 'b': [None, 3], 'c': [None, 45.98], 
                                         'd': [None, 3], 'e': [None, 44.8], 'f': [None, 0.0099]}), 
                    'address': OrderedDict({'specification': [None, '!sam1'], 'fileLoc': [None, '/home/sam/lib/data'], 'g': [None, 'good memories'], 
                                            'h': [None, 91.8], 'i': [None, 3], 'j': [None, 4], 'k': [None, 5], 'l': [None, 11.0], 'm': [None, 999]})})
    plug = Dict(collection=my_dict)
    plug.add_rows()
    assert len(plug.output_collector) == 5
    assert type(plug.output_collector) == OrderedDict
    for inner_dict, val in plug.output_collector.items():
        if inner_dict == '"2"':
            assert len(val) == 7
        elif inner_dict == 'all':
            assert len(val) == 9
            assert 'G' in val.keys()
        elif inner_dict == 'physics':
            assert len(val) == 7
        elif inner_dict == 'math':
            assert len(val) == 7
        elif inner_dict == 'address':
            assert len(val) == 9

def test_dict_reader():
    my_dict = {'"2"': {'specification': ['!jack'], 'a': [1], 'b': [2], 'c': [45.98], 'd': [2], 'e': [34.8], 'f': [0.0089]}, 
                    'all': {'specification': ['!sam'], 'fileLoc': ['/home/sam/lib/data'], 'G': ['good memories'], 
                                        'all': [9.8], 'i': [2], 'j': [3], 'k': [4], 'l': [1.0], 'm': [99]}, 
                    'physics': {'specification': ['!amy', '!amy1'], 'n': [9.8, 91.8], 'o': ['gravity', 'gravity'], 
                                        'p': [23, 233], 'q': ['home 23', 'home 23'], 'r': [1, 12], 's': [-0.0012, -0.0122]}, 
                    'math': {'specification': [None, '!jack1'], 'a': [None, 2], 'b': [None, 3], 'c': [None, 45.98], 
                                        'd': [None, 3], 'e': [None, 44.8], 'f': [None, 0.0099]}, 
                    'address': {'specification': [None, '!sam1'], 'fileLoc': [None, '/home/sam/lib/data'], 'g': [None, 'good memories'], 
                                        'h': [None, 91.8], 'i': [None, 3], 'j': [None, 4], 'k': [None, 5], 'l': [None, 11.0], 'm': [None, 999]}}
    plug = Dict(collection=my_dict)
    plug.add_rows()
    assert len(plug.output_collector) == 5
    assert type(plug.output_collector) == OrderedDict
    for inner_dict, val in plug.output_collector.items():
        if inner_dict == '"2"':
            assert len(val) == 7
        elif inner_dict == 'all':
            assert len(val) == 9
            assert 'G' in val.keys()
        elif inner_dict == 'physics':
            assert len(val) == 7
        elif inner_dict == 'math':
            assert len(val) == 7
        elif inner_dict == 'address':
            assert len(val) == 9