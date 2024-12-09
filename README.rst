=============
DSI
=============

The goal of the Data Science Infrastructure Project (DSI) is to provide a flexible, AI-ready metadata query capability which returns data subject to strict, POSIX-enforced file security. The data lifecycle for AI/ML requires seamless transitions from data-intensive/AI/ML research activity to long-term archiving and shared data repositories. DSI enables flexible, data-intensive scientific workflows that meet researcher needs.

DSI is implemented in three parts:

* Plugins (Readers and Writers)
* Backends
* Core middleware

Plugins curate metadata for query and data return. Plugins can have read or write funcitonality acting as Readers and Writers for DSI. Plugins acting as readers harvest data from files and streams. Plugins acting as writers execute containerized or baremetal applications to supplement queriable metadata and data. Plugins may be user contributed and a default set of plugins is available with usage examples in our `Core documentation <https://lanl.github.io/dsi/core.html>`_.

Backends are interfaces for the Core middleware. Backends consist mostly of back-end/storage functionalities and are the interface between the Core Middleware and a data store. Backends may also have some front-end functionality interfacing between a DSI user and the Core middleware. Backends may be user contributed and a default set of backends are available with usage examples in our `Core documentation <https://lanl.github.io/dsi/core.html>`_.

DSI Core middleware provides the user/machine interface. The Core middleware defines a Terminal object. An instantiated Core Terminal can load zero or more plugins and backends. A Terminal object can be used in scripting workflows and program loops.

=====================
DSI Core Requirements
=====================
* python3 (3.11 tested)
* Cross-platform (Unix / macOS / Windows)
* Git
* Plugins and Backends introduce further requirements

===============
Getting Started
===============

DSI has several versioned releases and cloning from 'main' can be considered as alpha-versions. Project contributors are encouraged to prototype solutions which do not contain sensitive data at this time. It is possible to install DSI locally instead with the following.

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

Install release versions of DSI can be found in (https://pypi.org/project/dsi-workflow/), to install the latest try the following::

	python3 -m pip install dsi-workflow

=====================
Copyright and License
=====================

This program is open source under the BSD-3 License.

© 2023. Triad National Security, LLC. All rights reserved.

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
