# convoy-rbd

./convoy daemon --drivers rbd --driver-opts ceph.defaultvolumesize=2048 --driver-opts ceph.defaultfstype=xfs  --driver-opts ceph.config="/etc/ceph/ceph.conf" --driver-opts ceph.pool=rbd