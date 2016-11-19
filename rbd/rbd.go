package rbd

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"os/exec"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/convoy/util"
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"

	. "github.com/rancher/convoy/convoydriver"
	. "github.com/rancher/convoy/logging"
	"path/filepath"
	"os"
)

const (
	DRIVER_NAME        = "rbd"

	DRIVER_CONFIG_FILE = "rbd.cfg"
	VOLUME_CFG_PREFIX = "volume_"
	DRIVER_CFG_PREFIX = DRIVER_NAME + "_"
	CFG_POSTFIX       = ".json"
	SNAPSHOT_PATH = "snapshots"
	MOUNTS_DIR = "mounts"

	CEPH_CLUSTER		 = "ceph.cluster"
	CEPH_CONFIG		 = "ceph.config"
	CEPH_SECRET              = "ceph.secret"
	CEPH_DEFAULT_POOL 	 = "ceph.pool"
	CEPH_DEFAULT_VOLUME_SIZE = "ceph.defaultvolumesize"
	CEPH_DEFAULT_FS_TYPE	 = "ceph.defaultfstype"
	CEPH_DEFAULT_USER 	 = "ceph.user"
)

type Device struct {
	Root               string
	Cluster            string
	Config             string
	DefaultPool 	   string
	DefaultVolumeSize  int64
	DefaultImageFSType string
	Conn         	   *rados.Conn        // keep an open connection
	DefaultIoctx 	   *rados.IOContext   // context for default pool
}

// RBDVolume is the Docker concept which we map onto a Ceph RBD Image
type RBDVolume struct {
	Name   		string // RBD Image name
	Device 		string // local host kernel device (e.g. /dev/rbd1)
	Size   		int64
	Locker 		string // track the lock name
	FsType 		string
	Pool   		string
	ConfigPath  	string
}

type Driver struct {
	mutex      *sync.RWMutex
	rVolumes   map[string]*RBDVolume
	User       string
	//Secret     string
	Device
}

func isDebugEnabled() bool {
	return true
}

func generateError(fields logrus.Fields, format string, v ...interface{}) error {
	return ErrorWithFields("Ceph RBD", fields, format, v)
}

func init() {
	fmt.Printf("%s", "Test for debug.\n")
	logrus.Debugf("test debug.\n")
	Register(DRIVER_NAME, Init)
}

func Init(root string, config map[string]string) (ConvoyDriver, error) {
	logrus.Debugf("config:%v", config)

	cluster := config[CEPH_CLUSTER]
	cephConfigFile := config[CEPH_CONFIG]
	user := config[CEPH_DEFAULT_USER]
	//secret := config[CEPH_SECRET]
	defaultVolumePool := config[CEPH_DEFAULT_POOL]
	defaultVolumeSize := config[CEPH_DEFAULT_VOLUME_SIZE]
	defaultFSType := config[CEPH_DEFAULT_FS_TYPE]

	size, err := strconv.Atoi(defaultVolumeSize)
	if err != nil {
		panic(err)
	}

	dev := &Device{
		Root:               root,
		Cluster: 	    cluster,
		Config:		    cephConfigFile,
		DefaultPool: 	    defaultVolumePool,
		DefaultVolumeSize:  int64(size),
		DefaultImageFSType: defaultFSType,
	}

	d := &Driver{
		mutex:      &sync.RWMutex{},
		rVolumes:   map[string]*RBDVolume{},
		User:       user,
		//Secret:     secret,
		Device:     *dev,
	}

	d.connect()

	return d, nil
}

func (d *Driver) Name() string {
	return DRIVER_NAME
}

func (d *Driver) Info() (map[string]string, error) {
	return map[string]string{
		"Root":        d.Root,
		"RBDCluster":  d.Cluster,
		"RBDUser":     d.User,
		//"RBDSecret":   d.Secret,
		"DefaultPool": d.DefaultPool,
		"DefaultVolumeSize": strconv.FormatInt(d.DefaultVolumeSize, 10),
		"DefaultImageFSType": d.DefaultImageFSType,
	}, nil
}

func (d *Driver) VolumeOps() (VolumeOperations, error) {
	return d, nil
}

func (d *Driver) SnapshotOps() (SnapshotOperations, error) {
	return nil, fmt.Errorf("Doesn't support snapshot operations")
}

func (d *Driver) BackupOps() (BackupOperations, error) {
	return nil, fmt.Errorf("Doesn't support backup operations")
}

/*
	VolumeOperations is Convoy Driver volume related operations interface. Any
	Convoy Driver must implement this interface.

	type VolumeOperations interface {
		Name() string
		CreateVolume(req Request) error
		DeleteVolume(req Request) error
		MountVolume(req Request) (string, error)
		UmountVolume(req Request) error
		MountPoint(req Request) (string, error)
		GetVolumeInfo(name string) (map[string]string, error)
		ListVolume(opts map[string]string) (map[string]map[string]string, error)
	}
*/
func (d *Driver) CreateVolume(req Request) error {
	logrus.Debugf("INFO: Create(%q)", req)
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var (
		size int
		err  error
	)

	name := req.Name
	fstype := d.DefaultImageFSType
	pool := d.DefaultPool

	opts := req.Options
	size, err = d.getSize(opts, d.DefaultVolumeSize)
	if err != nil {
		return err
	}

	if req.Options["size"] != "" {
		size, err = strconv.Atoi(req.Options["size"])
		if err != nil {
			logrus.Debugf("WARN: using default size. unable to parse int from %s: %s", req.Options["size"], err)
		}
	}
	if req.Options["fstype"] != "" {
		fstype = req.Options["fstype"]
	}

	// check for mount
	mount := d.mountPoint(pool, name)

	// do we already know about this volume? return early
	if _, found := d.rVolumes[mount]; found {
		logrus.Debugf("INFO: Volume is already in known mounts: " + mount)
		return generateError(logrus.Fields{
			LOG_FIELD_VOLUME: name,
		}, "Volume is already in known mounts")
	}

	// otherwise, check ceph rbd api for it
	exists, err := d.rbdImageExists(pool, name)
	if err != nil {
		logrus.Debugf("ERROR: checking for RBD Image: %s", err)
		return err
	}
	if !exists {
		// try to create it ... use size and default fs-type
		err = d.createRBDImage(pool, name, size, fstype)
		if err != nil {
			errString := fmt.Sprintf("Unable to create Ceph RBD Image(%s): %s", name, err)
			logrus.Debugf("ERROR: " + errString)
			return err
		}
	}

	return nil
}

func (d *Driver) DeleteVolume(req Request) error {
	logrus.Debugf("INFO: Remove(%s)", req.Name)
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var err  error
	name := req.Name
	pool := d.DefaultPool
	mount := d.mountPoint(pool, name)

	// do we know about this volume? does it matter?
	if _, found := d.rVolumes[mount]; !found {
		logrus.Debugf("WARN: Volume is not in known mounts: %s", mount)
	}

	// check ceph rbd api for it
	exists, err := d.rbdImageExists(pool, name)
	if err != nil {
		logrus.Debugf("ERROR: checking for RBD Image: %s", err)
		return err
	}
	if !exists {
		errString := fmt.Sprintf("Ceph RBD Image not found: %s", name)
		logrus.Debugf("ERROR: " + errString)
		return err
	}

	// attempt to gain lock before remove - lock disappears after rm or rename
	locker, err := d.lockImage(pool, name)
	if err != nil {
		errString := fmt.Sprintf("Unable to lock image for remove: %s", name)
		logrus.Debugf("ERROR: " + errString)
		return err
	}

	// remove it (for real - destroy it ... )
	err = d.removeRBDImage(pool, name)
	if err != nil {
		errString := fmt.Sprintf("Unable to remove Ceph RBD Image(%s): %s", name, err)
		logrus.Debugf("ERROR: " + errString)
		defer d.unlockImage(pool, name, locker)
		return err
	}

	delete(d.rVolumes, mount)
	return nil
}

func (d *Driver) MountVolume(req Request) (string, error) {
	logrus.Debugf("INFO: Mount(%s)", req.Name)
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var err  error
	name := req.Name
	pool := d.DefaultPool

	mount := d.mountPoint(pool, name)
	locker, err := d.lockImage(pool, name)
	if err != nil {
		logrus.Debugf("ERROR: locking RBD Image(%s): %s", name, err)
		return "", err
	}

	// map and mount the RBD image -- these are OS level Ceph commands, not avail in go-ceph map
	device, err := d.mapImage(pool, name)
	if err != nil {
		logrus.Debugf("ERROR: mapping RBD Image(%s) to kernel device: %s", name, err)
		defer d.unlockImage(pool, name, locker)
		return "", err
	}

	// determine device FS type
	fsType, err := d.deviceType(device)
	if err != nil {
		logrus.Debugf("WARN: unable to detect RBD Image(%s) fstype: %s", name, err)
		// NOTE: don't fail - FOR NOW we will assume default plugin fstype
		fsType = d.DefaultImageFSType
	}

	// check for mountdir - create if necessary
	err = os.MkdirAll(mount, os.ModeDir|os.FileMode(int(0775)))
	if err != nil {
		logrus.Debugf("ERROR: creating mount directory: %s", err)
		// failsafe: need to release lock and unmap kernel device
		defer d.unmapImageDevice(device)
		defer d.unlockImage(pool, name, locker)
		return "", err
	}

	// mount
	err = d.mountDevice(device, mount, fsType)
	if err != nil {
		logrus.Debugf("ERROR: mounting device(%s) to directory(%s): %s", device, mount, err)
		// need to release lock and unmap kernel device
		defer d.unmapImageDevice(device)
		defer d.unlockImage(pool, name, locker)
		return "", err
	}

	// if all that was successful - add to our list of volumes
	d.rVolumes[mount] = &RBDVolume{
		Name:   name,
		Device: device,
		Locker: locker,
		FsType: fsType,
		Pool:   d.DefaultPool,
	}

	return mount, nil
}

func (d *Driver) UmountVolume(req Request) error {
	logrus.Debugf("INFO: Unmount(%s)", req.Name)
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var err_msgs = []string{}
	var err  error

	name := req.Name
	pool := d.DefaultPool

	mount := d.mountPoint(pool, name)

	// check if it's in our mounts - we may not know about it if plugin was started late?
	vol, found := d.rVolumes[mount]
	if !found {
		logrus.Debugf("WARN: Volume is not in known mounts: will attempt limited Unmount: " + name)
		// set up a fake Volume with defaults ...
		// - device is /dev/rbd/<pool>/<image> in newer ceph versions
		// - assume we are the locker (will fail if locked from another host)
		vol = &RBDVolume{
			Pool:   pool,
			Name:   name,
			Device: fmt.Sprintf("/dev/rbd/%s/%s", pool, name),
			Locker: d.localLockerCookie(),
		}
	}

	// unmount
	err = d.unmountDevice(vol.Device)
	if err != nil {
		logrus.Debugf("ERROR: unmounting device(%s): %s", vol.Device, err)
		err_msgs = append(err_msgs, "Error unmounting device")
	}

	// unmap
	err = d.unmapImageDevice(vol.Device)
	if err != nil {
		logrus.Debugf("ERROR: unmapping image device(%s): %s", vol.Device, err)
		err_msgs = append(err_msgs, "Error unmapping kernel device")
	}

	// unlock
	err = d.unlockImage(vol.Pool, vol.Name, vol.Locker)
	if err != nil {
		logrus.Debugf("ERROR: unlocking RBD image(%s): %s", vol.Name, err)
		err_msgs = append(err_msgs, "Error unlocking image")
	}

	// forget it
	delete(d.rVolumes, mount)

	// check for piled up errors
	if len(err_msgs) > 0 {
		logrus.Debugf("%s", strings.Join(err_msgs, ", "))
	}

	return nil
}

func (d *Driver) MountPoint(req Request) (string, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	name := req.Name
	mountPath := d.mountPoint(d.DefaultPool, name)
	logrus.Debugf("INFO: Path request(%s) => %s", name, mountPath)

	return mountPath, nil
}

func (d *Driver) GetVolumeInfo(name string) (map[string]string, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	pool := d.DefaultPool

	// Check to see if the image exists
	exists, err := d.rbdImageExists(pool, name)
	if err != nil {
		logrus.Debugf("WARN: checking for RBD Image: %s", err)
		return nil, err
	}
	mountPath := d.mountPoint(pool, name)
	if !exists {
		logrus.Debugf("WARN: Image %s does not exist", name)
		delete(d.rVolumes, mountPath)
		return nil, err
	}
	logrus.Debugf("INFO: Get request(%s) => %s", name, mountPath)
	// TODO: what to do if the mountPoint registry (d.volumes) has a different name?

	result := map[string]string{
		OPT_VOLUME_NAME:         name,
		OPT_MOUNT_POINT:         mountPath,
		}
	return result, nil
}

func (d *Driver) ListVolume(opts map[string]string) (map[string]map[string]string, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	var err error

	volumes := make(map[string]map[string]string)
	for k, v := range d.rVolumes {
		volumes[k], err = d.GetVolumeInfo(v.Name)
		if err != nil {
			return nil, err
		}
	}
	return volumes, nil
}

/*
	This section is the operations to Ceph service
*/
func (d *Driver) openContext(pool string) (*rados.IOContext, error) {
	// check default first
	if pool == d.DefaultPool && d.DefaultIoctx != nil {
		return d.DefaultIoctx, nil
	}
	// otherwise open new pool context ... call shutdownContext(ctx) to destroy
	ioctx, err := d.Conn.OpenIOContext(pool)
	if err != nil {
		// TODO: make sure we aren't hiding a useful error struct by casting to string?
		msg := fmt.Sprintf("Unable to open context(%s): %s", pool, err)
		logrus.Errorf("ERROR: " + msg)
		return ioctx, errors.New(msg)
	}
	return ioctx, nil
}

// localLockerCookie returns the Hostname
func (d *Driver) localLockerCookie() string {
	host, err := os.Hostname()
	if err != nil {
		logrus.Debugf("WARN: HOST_UNKNOWN: unable to get hostname: %s", err)
		host = "HOST_UNKNOWN"
	}
	return host
}

// shutdownContext will destroy any non-default ioctx
func (d *Driver) shutdownContext(ioctx *rados.IOContext) {
	if ioctx != nil && ioctx != d.DefaultIoctx {
		ioctx.Destroy()
	}
}

// connect builds up the ceph conn and default pool
func (d *Driver) connect() {
	logrus.Debugf("INFO: connecting to Ceph and default pool context")
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var err error
	if d.Cluster == "" {
		if d.User == "" {
			d.Conn, err = rados.NewConn()
		} else {
			d.Conn, err = rados.NewConnWithUser(d.User)
		}

	} else {
		// FIXME: TODO: can't seem to use a cluster name -- get error -22 from go-ceph/rados:
		// panic: Unable to create ceph connection to cluster=ceph with user=admin: rados: ret=-22
		d.Conn, err = rados.NewConnWithClusterAndUser(d.Cluster, d.User)
	}
	if err != nil {
		logrus.Debugf("ERROR: Unable to create ceph connection to cluster=%s with user=%s: %s", d.Cluster, d.User, err)
	}

	// read ceph.conf and setup connection
	if d.Config == "" {
		err = d.Conn.ReadDefaultConfigFile()
	} else {
		err = d.Conn.ReadConfigFile(d.Config)
	}
	if err != nil {
		logrus.Debugf("ERROR: Unable to read ceph config: %s", err)
	}

	err = d.Conn.Connect()
	if err != nil {
		logrus.Debugf("ERROR: Unable to connect to Ceph: %s", err)
	}

	// setup the default context (pool most likely to be used)
	defaultContext, err := d.openContext(d.DefaultPool)
	if err != nil {
		logrus.Debugf("ERROR: Unable to connect to default Ceph Pool: %s", err)
	}
	d.DefaultIoctx = defaultContext
}

// mapImage will map the RBD Image to a kernel device
func (d *Driver) mapImage(pool, imageName string) (string, error) {
	_, err := sh("rbd", "map", "--pool", pool, imageName)
	if err != nil{
		logrus.Debugf("ERROR: 'rbd map --pool %s %s' failed!", pool, imageName)
		return "", err
	}

	output, err := sh("rbd", "showmapped")
	lines := strings.Split(output, "\n")[1:]

	for _, line := range lines {
		words := strings.Split(line, " ")
		nw := make([]string, 0)
		for _, v := range words{
			v = strings.Replace(v, " ", "", -1)
			if v != "" {
				nw = append(nw, v)
			}
		}
		if pool == nw[1] && imageName == nw[2]{
			return nw[4], nil
		}
	}
	return "", err
}

// unmapImageDevice will release the mapped kernel device
func (d *Driver) unmapImageDevice(device string) error {
	_, err := sh("rbd", "unmap", device)
	return err
}

func (d *Driver) mountPoint(pool, name string) string {
	return filepath.Join(d.Root, pool, name)
}

// lockImage locks image and returns locker cookie name
func (d *Driver) lockImage(pool, imageName string) (string, error) {
	logrus.Debugf("INFO: lockImage(%s/%s)", pool, imageName)

	ctx, err := d.openContext(pool)
	if err != nil {
		return "", err
	}
	defer d.shutdownContext(ctx)

	// build image struct
	rbdImage := rbd.GetImage(ctx, imageName)

	// open it (read-only)
	err = rbdImage.Open(true)
	if err != nil {
		logrus.Debugf("ERROR: opening rbd image(%s): %s", imageName, err)
		return "", err
	}
	defer rbdImage.Close()

	// lock it using hostname
	locker := d.localLockerCookie()
	err = rbdImage.LockExclusive(locker)
	if err != nil {
		return locker, err
	}
	return locker, nil
}

// unlockImage releases the exclusive lock on an image
func (d *Driver) unlockImage(pool, imageName, locker string) error {
	if locker == "" {
		return errors.New(fmt.Sprintf("Unable to unlock image(%s/%s) for empty locker", pool, imageName))
	}
	logrus.Debugf("INFO: unlockImage(%s/%s, %s)", pool, imageName, locker)

	ctx, err := d.openContext(pool)
	if err != nil {
		return err
	}
	defer d.shutdownContext(ctx)

	// build image struct
	rbdImage := rbd.GetImage(ctx, imageName)

	// open it (read-only)
	err = rbdImage.Open(true)
	if err != nil {
		logrus.Debugf("ERROR: opening rbd image(%s): %s", imageName, err)
		return err
	}
	defer rbdImage.Close()
	return rbdImage.Unlock(locker)
}

// rbdImageExists will check for an existing Ceph RBD Image
func (d *Driver) rbdImageExists(pool, findName string) (bool, error) {
	logrus.Debugf("INFO: rbdImageExists(%s/%s)", pool, findName)
	if findName == "" {
		return false, fmt.Errorf("Empty Ceph RBD Image name")
	}

	ctx, err := d.openContext(pool)
	if err != nil {
		return false, err
	}
	defer d.shutdownContext(ctx)

	img := rbd.GetImage(ctx, findName)
	err = img.Open(true)
	defer img.Close()
	if err != nil {
		if err == rbd.RbdErrorNotFound {
			logrus.Debugf("INFO: Ceph RBD Image ('%s') not found: %s", findName, err)
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// createRBDImage will create a new Ceph block device and make a filesystem on it
func (d *Driver) createRBDImage(pool string, name string, size int, fstype string) error {
	logrus.Debugf("INFO: Attempting to create RBD Image: (%s, %s, %s)", name, size, fstype)

	// check that fs is valid type (needs mkfs.fstype in PATH)
	mkfs, err := exec.LookPath("mkfs." + fstype)
	if err != nil {
		msg := fmt.Sprintf("Unable to find mkfs for %s in PATH: %s", fstype, err)
		return errors.New(msg)
	}

	// create the block device image with format=2
	//  should we enable all v2 image features?: +1: layering support +2: striping v2 support +4: exclusive locking support +8: object map support
	// NOTE: i tried but "2015-08-02 20:24:36.726758 7f87787907e0 -1 librbd: librbd does not support requested features."
	// NOTE: I also tried just image-features=4 (locking) - but map will fail:
	//       sudo rbd unmap mynewvol =>  rbd: 'mynewvol' is not a block device, rbd: unmap failed: (22) Invalid argument
	//	"--image-features", strconv.Itoa(4),
	_, err = sh(
		"rbd", "create",
		"--image-format", strconv.Itoa(2),
		"--pool", pool,
		"--size", strconv.Itoa(size),
		name,
	)
	if err != nil {
		return err
	}

	// lock it temporarily for fs creation
	lockname, err := d.lockImage(pool, name)
	if err != nil {
		// TODO: defer image delete?
		return err
	}

	// map to kernel device
	device, err := d.mapImage(pool, name)
	if err != nil {
		defer d.unlockImage(pool, name, lockname)
		return err
	}

	// make the filesystem
	_, err = sh(mkfs, device)
	if err != nil {
		defer d.unmapImageDevice(device)
		defer d.unlockImage(pool, name, lockname)
		return err
	}

	// TODO: should we now just defer both of unmap and unlock? or catch err?
	//
	// unmap
	err = d.unmapImageDevice(device)
	if err != nil {
		// ? if we cant unmap -- are we screwed? should we unlock?
		return err
	}

	// unlock
	err = d.unlockImage(pool, name, lockname)
	if err != nil {
		return err
	}

	return nil
}

// removeRBDImage will remove a Ceph RBD image - no undo available
func (d *Driver) removeRBDImage(pool, name string) error {
	logrus.Debugf("INFO: Remove RBD Image(%s/%s)", pool, name)

	// remove the block device image
	_, err := sh(
		"rbd", "rm",
		"--pool", pool,
		name,
	)
	if err != nil {
		return err
	}
	return nil
}

// deviceType identifies Image FS Type - requires RBD image to be mapped to kernel device
func (d *Driver) deviceType(device string) (string, error) {
	// blkid Output:
	//	/dev/rbd3: xfs
	blkid, err := sh("blkid", "-o", "value", "-s", "TYPE", device)
	if err != nil {
		return "", err
	}
	if blkid != "" {
		return blkid, nil
	} else {
		return "", errors.New("Unable to determine device fs type from blkid")
	}
}

// mountDevice will call mount on kernel device with a docker volume subdirectory
func (d *Driver) mountDevice(device, mountdir, fstype string) error {
	_, err := sh("mount", "-t", fstype, device, mountdir)
	return err
}

// unmountDevice will call umount on kernel device to unmount from host's docker subdirectory
func (d *Driver) unmountDevice(device string) error {
	_, err := sh("umount", device)
	return err
}

func (d *Driver) getSize(opts map[string]string, defaultVolumeSize int64) (int, error) {
	size := opts[OPT_SIZE]
	if size == "" || size == "0" {
		size = strconv.FormatInt(defaultVolumeSize, 10)
	}
	size64, err := util.ParseSize(size)
	return int(size64), err
}

// sh is a simple os.exec Command tool, returns trimmed string output
func sh(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	if isDebugEnabled() {
		logrus.Debug("DEBUG: sh CMD: %q", cmd)
	}
	// TODO: capture and output STDERR to logfile?
	out, err := cmd.Output()
	return strings.Trim(string(out), " \n"), err
}