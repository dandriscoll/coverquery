This system, named CoverQuery, is designed to manage code coverage data for multiple projects and allow a coding LLM to query which tests to run and which parts of the code are covered.

Directory Structure:

Inside each project, there is a .coverquery directory.

This .coverquery directory contains:

A set of per-run subdirectories where temporary coverage results are stored before being integrated into the index.

A .pid file that tracks the process ID of the currently running CoverQuery watcher, allowing for easy process management.

Command-Line Tools:

Start the Watcher:

Command: coverquery start

This command starts the file system watcher, which reads the project’s configuration and begins monitoring the project directory for changes. It will write the .pid file to the .coverquery directory so that the running process can be easily managed.

Manually Trigger a Run:

Command: coverquery run

This command allows a developer to manually trigger the test run and coverage collection at any time. It will run the tests, collect the coverage data, and store the results in a new run-specific subdirectory under .coverquery.

List Tests Without Running:

Internally, CoverQuery uses pytest --collect-only to list all available tests without executing them. This allows CoverQuery to know exactly which tests are available and to determine which individual tests to run based on the coverage information.

In addition to the existing command-line tools and directory structure, we will also create a local DEB package for easy installation on a Linux system.

Creating the DEB Package:

We will package CoverQuery as a DEB file so it can be installed locally using apt or dpkg. This package will place the CoverQuery executable in a standard location like /usr/local/bin and install its supporting files under /usr/local/lib/coverquery.

The DEB package will also handle installing any necessary dependencies that are available via the system package manager, ensuring that CoverQuery has everything it needs.

Installation Using the DEB:

Once the DEB package is created, it can be installed locally with a simple sudo apt install ./coverquery.deb. This will put all the files in the correct places and set up CoverQuery for use on that machine.

After installation, the user can simply run coverquery start or coverquery run as usual, and everything will work out of the box, without needing to set up virtual environments manually.