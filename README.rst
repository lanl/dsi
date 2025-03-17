=============
DSI
=============

The goal of the Data Science Infrastructure Project (DSI) is to provide a flexible, 
AI-ready metadata query capability which returns data subject to strict, POSIX-enforced file security. 
The data lifecycle for AI/ML requires seamless transitions from data-intensive/AI/ML research activity to long-term archiving and shared data repositories. 
DSI enables flexible, data-intensive scientific workflows that meet researcher needs.

DSI is implemented in three parts:

* `Plugins (Readers and Writers) <https://lanl.github.io/dsi/plugins.html>`_
* `Backends <https://lanl.github.io/dsi/backends.html>`_
* `Core middleware <https://lanl.github.io/dsi/core.html>`_

Plugins curate data and metadata for query and data return. 
Plugins can either be Readers to read data into DSI or Writers to export data from DSI into another format. 
Users can contribute to our Plugins with a default set available in our `Plugin documentation <https://lanl.github.io/dsi/plugins.html>`_.

Backends are interfaces between the Core middleware and a data storage system. 
While they mostly consist of storage functionalities, users may interact with them to either ingest, process (read), query, or find data.
Users can also generate Python notebooks from certain Backends to visually interact with the data as well.
Users can contribute to our Backends with a default set available in our `Backend documentation <https://lanl.github.io/dsi/backends.html>`_.

DSI Core middleware provides the user/machine interface. 
The Core middleware defines a Terminal object. 
An instantiated Core Terminal can load zero or more plugins and backends. 
A Terminal object can be used in scripting workflows and program loops.
Users should view our `Core documentation <https://lanl.github.io/dsi/core.html>`_ to see how to use the Core Terminal object to interact with Plugins and Backends

=====================
DSI Baseline Requirements
=====================
* python3 (3.11 tested)
* Cross-platform (Unix / macOS / Windows)
* Git
* Plugins and Backends introduce further requirements

===============
Getting Started
===============

DSI has several versioned releases and cloning from 'main' can be considered the alpha-version. 
Project contributors are encouraged to prototype solutions which do not contain sensitive data at this time. 
It is possible to install DSI locally instead with the following.

We recommend Miniconda3 for managing virtual environments for DSI::

	. ~/miniconda3/bin/activate
	conda create -n dsi python=3.11
	conda activate dsi

Python virtual environments can also be used for DSI::

	python3 -m venv dsienv
	source dsienv/bin/activate
	pip install --upgrade pip

After activating your environment::

	git clone https://github.com/lanl/dsi.git
	cd dsi/
	python3 -m pip install .
	
=====================
Release Versions
=====================

Install release versions of DSI can be found in (https://pypi.org/project/dsi-workflow/). To install the latest version, try the following::

	python3 -m pip install dsi-workflow

=====================
Copyright and License
=====================

This program is open source under the BSD-3 License.

© 2025. Triad National Security, LLC. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted
provided that the following conditions are met:

1.Redistributions of source code must retain the above copyright notice, this list of conditions and
the following disclaimer.
 
2.Redistributions in binary form must reproduce the above copyright notice, this list of conditions
and the following disclaimer in the documentation and/or other materials provided with the
distribution.
 
3.Neither the name of the copyright holder nor the names of its contributors may be used to endorse
or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
