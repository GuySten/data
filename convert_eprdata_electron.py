#!/usr/bin/env python3

"""
Convert an MCNP electron-photon-relaxation ACE library (eprdata12, eprdata14,
or later) into an HDF5 electron library for OpenMC.

An EPR library is a single ACE file with one table per element. Only the
electron data is written; the photoatomic and relaxation data in the same
tables is ignored.
"""

import argparse
from pathlib import Path

import openmc.data

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('epr', type=Path,
                    help='Path to the EPR ACE library (e.g. eprdata14)')
parser.add_argument('-d', '--destination', type=Path, default=Path('electron'),
                    help='Directory to write the HDF5 files to')
parser.add_argument('--libver', choices=['earliest', 'latest'],
                    default='earliest', help='Output HDF5 versioning')
args = parser.parse_args()

(args.destination / 'electron').mkdir(parents=True, exist_ok=True)

library = openmc.data.DataLibrary()

for table in openmc.data.ace.Library(args.epr).tables:
    print(f'Converting: {table.name}')
    data = openmc.data.IncidentElectron.from_ace(table)

    h5_file = args.destination / 'electron' / f'{data.name}.h5'
    print(f'Writing {h5_file}...')
    data.export_to_hdf5(h5_file, 'w', libver=args.libver)

    library.register_file(h5_file)

library.export_to_xml(args.destination / 'cross_sections.xml')
