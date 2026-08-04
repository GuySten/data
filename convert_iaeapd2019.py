#!/usr/bin/env python3

"""
Convert IAEA-PD2019 ACE data from iaea nds distribution into an HDF5 library
that can be used by OpenMC.
"""

import argparse
from collections import defaultdict
from pathlib import Path
import sys

import openmc.data


# Make sure Python version is sufficient
assert sys.version_info >= (3, 6), "Python 3.6+ is required"



class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter,
                      argparse.RawDescriptionHelpFormatter):
    pass


parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=CustomFormatter
)
parser.add_argument('-d', '--destination', type=Path, default=Path('iaea-pd2019'),
                    help='Directory to create new library in')
parser.add_argument('--libver', choices=['earliest', 'latest'],
                    default='earliest', help="Output HDF5 versioning. Use "
                    "'earliest' for backwards compatibility or 'latest' for "
                    "performance")
parser.add_argument('mcnpdata', type=Path,
                    help='Directory containing photonuclear ACE files')
args = parser.parse_args()

# Check arguments to make sure they're valid
assert args.mcnpdata.is_dir(), 'mcnpdata argument must be a directory'

# Get a list of all ACE files
iaea2019 = list(args.mcnpdata.glob('*.acef'))

# Create output directory if it doesn't exist
args.destination.mkdir(parents=True, exist_ok=True)

library = openmc.data.DataLibrary()


# Create output directory if it doesn't exist
(args.destination / 'photonuclear').mkdir(parents=True, exist_ok=True)

for path in iaea2019:
    lib = openmc.data.ace.Library(path)
    for table in lib.tables:
        print(f'Converting: {table.name}')
        data = openmc.data.IncidentPhotonuclear.from_ace(table, 'mcnp')

        # Export HDF5 file
        h5_file = args.destination / 'photonuclear' / f'{data.name}.h5'
        print(f'Writing {h5_file}...')
        data.export_to_hdf5(h5_file, 'w', libver=args.libver)

        # Register with library
        library.register_file(h5_file)

# Write cross_sections.xml
library.export_to_xml(args.destination / 'cross_sections.xml')
