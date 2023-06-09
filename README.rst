=============
DSI
=============

.. image:: coverage.svg
      :target: https://lanl.github.io/dsi/htmlcov/index.html

The goal of the Data Science Infrastructure Project (DSI) is to provide a flexible, AI-ready metadata query capability which returns data subject to strict, POSIX-enforced file security. The data lifecycle for AI/ML requires seamless transitions from data-intensive/AI/ML research activity to long-term archiving and shared data repositories. DSI enables flexible, data-intensive scientific workflows that meet researcher needs.

DSI is implemented in three parts:

* Plugins
* Drivers
* Core middleware

Plugins curate metadata for query and data return. Plugins can have producer or consumer funcitonality. Plugins acting as consumers harvest data from files and streams. Plugins acting as producers execute containerized or baremetal applications to supplement queriable metadata and data. Plugins may be user contributed and a default set of plugins is available with usage examples in our `Core documentation <https://lanl.github.io/dsi/core.html>`_.

Drivers are interfaces for the Core middleware. Drivers can have front-end or back-end functionalities. Driver front-ends are the interface between a DSI user and the Core middleware. Driver back-ends are the interface between the Core Middleware and a data store. Drivers may be user contributed and a default set of drivers is available with usage examples in our `Core documentation <https://lanl.github.io/dsi/core.html>`_.

DSI Core middleware provides the user/machine interface. The Core middleware defines a Terminal object. An instantiated Core Terminal can load zero or more plugins and drivers. A Terminal object can be used in scripting workflows and program loops.

=====================
DSI Core Requirements
=====================
* python3 (3.11 tested)
* Linux OS (RHEL- and Debian-based distributions tested)
* Git
* Plugins and Drivers introduce further requirements

===============
Getting Started
===============

DSI does not yet have a versioned release and should be considered pre-alpha. Project contributors are encouraged to prototype solutions which do not contain sensitive data at this time. Consequently a PyPA release is planned but incomplete. It is possible to install DSI locally instead.

We recommend Miniconda3 for managing virtual environments for DSI::

	. ~/miniconda3/bin/activate
	conda create -n dsi python=3.11
	conda activate dsi

After activating your environment::

	git clone https://github.com/lanl/dsi.git
	cd dsi/
	python -m pip install .
	

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
