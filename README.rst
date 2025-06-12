=============
DSI
=============

The goal of the Data Science Infrastructure Project (DSI) is to provide a flexible, 
AI-ready metadata query capability which returns data subject to strict, POSIX-enforced file security. 
The data lifecycle for AI/ML requires seamless transitions from data-intensive/AI/ML research activity to long-term archiving and shared data repositories. 
DSI enables flexible, data-intensive scientific workflows that meet researcher needs.

DSI is implemented in three parts:

* Readers and Writers
* Backends 
* Core middleware

For more information on these DSI modules, please refer to the `DSI Introduction Page <https://lanl.github.io/dsi/intro-users.html>`_.

Users can interact with DSI seamlessly through two methods:

* `Python API <https://lanl.github.io/dsi/python_api.html>`_ for more flexibility when loading/exporting data
* `Command Line Interface API <https://lanl.github.io/dsi/cli_api.html>`_ for streamlined DSI functionality without requiring any knowledge of Python


========================
DSI Base Requirements
========================
* python3 (3.11 tested)
* Cross-platform (Unix / macOS / Windows)
* Git
* Plugins and Backends introduce further requirements

===============
Installation
===============

DSI has several versioned releases and cloning from 'main' is the alpha-version. 

Users that want to install DSI should follow our `installation steps <https://lanl.github.io/dsi/installation.html>`_.
	
=====================
Release Versions
=====================

Supported release versions of DSI are tagged and found in (https://github.com/lanl/dsi/releases). Releases with minimal requirements are published on *pip* via (https://pypi.org/project/dsi-workflow/). To install the latest version, try the following::

	python3 -m pip install dsi-workflow

=====================
Contributor Resources
=====================
DSI Contributors can view our more developer-focused documentation in the `Contributor Resources <https://lanl.github.io/dsi/contributors.html>`_.

These pages further explain DSI Readers/Writers/Backends, as well as how to write your own Reader/Writer compatible with DSI.
Project contributors are encouraged to prototype solutions which do not contain sensitive data at this time. 

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
