# This Makefile needs bzip2, gzip, xz, 7z, tar, rar and python

BASE = x.csv y.csv z.json
COMPRESSED = x.csv.xz x.csv.gz x.csv.bz2 \
 			 y.csv.xz y.csv.gz y.csv.bz2
ARCHIVES = xy.zip xy.7z xy.rar xy.tar
COMPRESSED_ARCHIVES = xy.tar.xz xy.tar.gz xy.tar.bz2
DATABASES = xy.db xy.xlsx y.h5
COMPOSITE = zz.zip zz.7z zz.rar zz.tar zz.tar.xz zz.tar.gz zz.tar.bz2
DERIVED_DATASETS = $(DERIVED_BASE) $(COMPRESSED) $(ARCHIVES) $(COMPRESSED_ARCHIVES) $(DATABASES) $(COMPOSITE)

datasets: $(DERIVED_DATASETS)

clean:
	rm -f $(DERIVED_DATASETS)

# Rules to create all compression formats.
# Note: --keep is not available on travis environment, so writing to stdout
%.bz2: %
	7z a -mx -y -tbzip2 $@ $+
%.gz: %
	7z a -mx -y -tgzip $@ $+
%.xz: %
	7z a -mx -y -txz $@ $+

# For each target in $(ARCHIVES) create the output
xy.tar: $(BASE)
	tar --posix -cf $@ $+
xy.7z: $(BASE)
	7z a -y -bd -mx $@ $+
xy.zip: $(BASE)
	7z a -y -bd -mx $@ $+
xy.rar: $(BASE)
	rar a -m5 $@ $+

# For each target in $(DATABASES) create the output
xy.db: x.csv y.csv
	python make_dataset.py $@ $+
# HDF5 likes homogenous non-unicode files. Feed it ASCII content
y.h5: y.csv
	python make_dataset.py $@ $+
xy.xlsx: x.csv y.csv
	python make_dataset.py $@ $+

# Create for each composite target
zz.tar: $(BASE) $(COMPRESSED) $(COMPRESSED_ARCHIVES) $(ARCHIVES) $(DATABASES)
	tar --posix -cf $@ $+
zz.7z: $(BASE) $(COMPRESSED) $(COMPRESSED_ARCHIVES) $(ARCHIVES) $(DATABASES)
	7z a -y -bd -mx $@ $+
zz.zip: $(BASE) $(COMPRESSED) $(COMPRESSED_ARCHIVES) $(ARCHIVES) $(DATABASES)
	7z a -y -bd -mx $@ $+
zz.rar: $(BASE) $(COMPRESSED) $(COMPRESSED_ARCHIVES) $(ARCHIVES) $(DATABASES)
	rar a -m5 $@ $+
