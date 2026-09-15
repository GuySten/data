#!/usr/bin/env python

from pathlib import Path
import tarfile

import numpy as np
import h5py

from utils import download


base_url = 'http://geant4.cern.ch/support/source/'
version = '6.48'
filename = f'G4EMLOW.{version}.tar.gz'

# ==============================================================================
# DOWNLOAD FILES FROM GEANT4 SITE

download(base_url + filename)

# ==============================================================================
# EXTRACT FILES FROM TGZ

g4dir = Path(f'G4EMLOW{version}')
if not g4dir.is_dir():
    with tarfile.open(filename, 'r') as tgz:
        print(f'Extracting {filename}...')
        tgz.extractall()

# ==============================================================================
# READ SHELL OCCUPANCIES
#
# The Compton profiles in G4EMLOW are the Hartree-Fock profiles of Biggs,
# Mendelsohn and Mann, At. Data Nucl. Data Tables 16, 201-309 (1975). The rows of
# profile-<Z>.dat, and the binding energies in column 2 of shell-doppler.dat, are
# ordered by increasing (n, l, j), the order in which Biggs tabulates the
# subshells. The occupancies in column 1 of shell-doppler.dat are not: they follow
# the ordering of Geant4's G4AtomicShells table, which sorts subshells by
# decreasing binding energy. For Z = 43 and 59-92 the 4f subshells are less
# tightly bound than 5s and 5p, so the two orderings diverge and the occupancy on
# a given line belongs to a different subshell than the binding energy beside it.
#
# The occupancies therefore come from compton_shell_occupancies.dat, which carries
# Biggs' own occupancies in Biggs' own order. See that file for provenance.

occupancy_file = Path(__file__).parent / 'compton_shell_occupancies.dat'
table = np.loadtxt(occupancy_file)
num_electrons = {z: table[table[:, 0] == z, 2] for z in range(1, 101)}
profile_j0 = {z: table[table[:, 0] == z, 3] for z in range(1, 101)}

# ==============================================================================
# GENERATE COMPTON PROFILE HDF5 FILE

print('Generating compton_profiles.h5...')

shell_file = g4dir / 'doppler' / 'shell-doppler.dat'

with open(shell_file, 'r') as shell, h5py.File('compton_profiles.h5', 'w') as f:
    # Read/write electron momentum values
    pz = np.loadtxt(g4dir / 'doppler' / 'p-biggs.dat')
    f.create_dataset('pz', data=pz)

    for z in range(1, 101):
        # Create group for this element
        group = f.create_group(f'{z:03}')

        # Read data into one long array
        path = g4dir / 'doppler' / f'profile-{z}.dat'
        with open(path, 'r') as profile:
            j = np.fromstring(profile.read(), sep=' ')

        # Determine number of electron shells and reshape. Profiles are
        # tabulated against a grid of 31 momentum values.
        n_shells = j.size // 31
        j.shape = (n_shells, 31)

        # Write Compton profile for this Z
        group.create_dataset('J', data=j)

        # Determine binding energies for each shell. The occupancies in column 1
        # are read past and discarded; see above.
        binding_energy = []
        while True:
            words = shell.readline().split()
            if words[0] == '-1':
                break
            binding_energy.append(float(words[1]))

        # Each occupancy was matched to a specific profile, so check the profiles
        # are still the ones it was matched to. This is what went wrong with the
        # occupancies in shell-doppler.dat, so fail loudly rather than write a
        # file in which the two are silently misaligned again.
        if len(num_electrons[z]) != n_shells:
            raise ValueError(
                f'Z={z}: profile-{z}.dat has {n_shells} shells but '
                f'{occupancy_file.name} has {len(num_electrons[z])}'
            )
        if not np.allclose(profile_j0[z], j[:, 0], rtol=1e-3, atol=0):
            raise ValueError(
                f'Z={z}: Compton profiles do not match the ones the shell '
                f'occupancies in {occupancy_file.name} were derived against'
            )
        if len(binding_energy) != n_shells:
            raise ValueError(
                f'Z={z}: shell-doppler.dat has {len(binding_energy)} shells '
                f'but profile-{z}.dat has {n_shells}'
            )
        if num_electrons[z].sum() != z:
            raise ValueError(
                f'Z={z}: occupancies sum to {num_electrons[z].sum()}'
            )

        # Write binding energies and number of electrons
        group.create_dataset('num_electrons', data=num_electrons[z])
        group.create_dataset('binding_energy', data=binding_energy)
