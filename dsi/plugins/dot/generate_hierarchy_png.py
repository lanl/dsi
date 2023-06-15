from abc import ABC
from dsi.plugins.structured_metadata import Plugin

import os
import sys
import graphviz

# This is a hacky way to import all modules in the parent directory
sys.path.insert(0, "../")
modules = []
for module in os.listdir(os.path.dirname(os.path.dirname(__file__))):
    if module[-3:] == ".py" and module != "generate_hierarchy_png.py":
        modules.append(module[:-3]) 
        __import__(module[:-3], locals(), globals())
print("Imported the following modules to find class hierarchy:", ", ".join(modules), "\n")


class ClassTreeNode:
    """
    Class tree structure that forms a hierarchy from one superclass
    """
    def __init__(self, clas):
        """
        Track the current class and recursively track subclasses
        """
        self.clas = clas
        self.subclasses = [ClassTreeNode(c) for c in clas.__subclasses__()]

    def export_png(self, name: str) -> None:
        """ Generate a dotfile and export class hierarchy to png """
        dot = graphviz.Digraph(name, format="png")
        root = self
        dot.node(root.clas.__name__)
        
        def process_children(r):
            for ch in r.subclasses:
                dot.node(ch.clas.__name__)
                dot.edge(r.clas.__name__, ch.clas.__name__)
                process_children(ch)
        
        process_children(root)
        print("Rendering the following dot source:") 
        print(dot.source)
        dot.render()
        print("done.")
        


if __name__ == "__main__":
    ct = ClassTreeNode(Plugin)  # generate class hierarchy for Plugin
    ct.export_png(name="PluginClassHierarchy")

