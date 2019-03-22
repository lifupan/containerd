// +build linux,!no_rawblock

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package rawblock

import (
	"context"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"

	"github.com/containerd/continuity/fs"
	"github.com/pkg/errors"
)

func init() {
	plugin.Register(&plugin.Registration{
		Type: plugin.SnapshotPlugin,
		ID:   "rawblock",
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			ic.Meta.Platforms = append(ic.Meta.Platforms, platforms.DefaultSpec())
			return NewSnapshotter(ic.Context, ic.Root)
		},
	})
}

type SnapshotterConfig struct {
	SizeMB  int64
	Fstype  string
	Options []string
}

type snapshotter struct {
	root    string
	sizeMB  int64
	fstype  string
	options string
	ms      *storage.MetaStore
}

func testReflinkCapability(dir string) (bool, error) {
	file1 := filepath.Join(dir, "test-reflink-support-src")
	file2 := filepath.Join(dir, "test-reflink-support-dst")
	if out, err := exec.Command("truncate", "--size=1024", file1).CombinedOutput(); err != nil {
		return false, errors.Errorf("%s:%v", string(out), err)
	}
	defer os.RemoveAll(file1)
	defer os.RemoveAll(file2)
	if _, err := exec.Command("cp", "-a", "--reflink=always", file1, file2).CombinedOutput(); err != nil {
		return false, nil
	}
	return true, nil
}

// NewSnapshotter returns a Snapshotter which copies layers on the underlying
// file system. A metadata file is stored under the root.
//
// Rawblock snapshot layout: each type of snapshots lives in its own directory.
// root dir -> Active
//          -> View
//          -> Committed
func NewSnapshotter(ctx context.Context, root string) (snapshots.Snapshotter, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}

	var (
		active    = filepath.Join(root, snapshots.KindActive.String())
		view      = filepath.Join(root, snapshots.KindView.String())
		committed = filepath.Join(root, snapshots.KindCommitted.String())
	)

	for _, path := range []string{
		active,
		view,
		committed,
	} {
		if err := os.Mkdir(path, 0755); err != nil && !os.IsExist(err) {
			return nil, err
		}
	}

	if supported, err := testReflinkCapability(active); err != nil {
		return nil, errors.Wrap(err, "failed to check host file system reflink capability")
	} else if !supported {
		log.G(ctx).Infof("%s doesn't support copy reflink. Snapshot creation can be SLOW!", root)
	}

	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, err
	}

	// TODO: read from SnapshotterConfig
	return &snapshotter{
		root:   root,
		sizeMB: 1024,
		fstype: "ext4",
		ms:     ms,
	}, nil
}

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (o *snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Info{}, err
	}
	defer t.Rollback()
	_, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

// Update updates the info for a snapshot.
//
// Only mutable properties of a snapshot may be updated.
func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return snapshots.Info{}, err
	}

	info, err = storage.UpdateInfo(ctx, info, fieldpaths...)
	if err != nil {
		t.Rollback()
		return snapshots.Info{}, err
	}

	if err := t.Commit(); err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

// Usage returns the resource usage of an active or committed snapshot
// excluding the usage of parent snapshots.
//
// The running time of this call for active snapshots is dependent on
// implementation, but may be proportional to the size of the resource.
// Callers should take this into consideration. Implementations should
// attempt to honer context cancellation and avoid taking locks when making
// the calculation.
func (o *snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Usage{}, err
	}
	defer t.Rollback()

	id, info, usage, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Usage{}, err
	}

	if info.Kind == snapshots.KindActive {
		du, err := fs.DiskUsage(ctx, o.getSnapshotPath(id, info.Kind))
		if err != nil {
			return snapshots.Usage{}, err
		}
		usage = snapshots.Usage(du)
	}

	return usage, nil
}

// Prepare creates an active snapshot identified by key descending from the
// provided parent.  The returned mounts can be used to mount the snapshot
// to capture changes.
//
// If a parent is provided, after performing the mounts, the destination
// will start with the content of the parent. The parent must be a
// committed snapshot. Changes to the mounted destination will be captured
// in relation to the parent. The default parent, "", is an empty
// directory.
//
// The changes may be saved to a committed snapshot by calling Commit. When
// one is done with the transaction, Remove should be called on the key.
//
// Multiple calls to Prepare or View with the same key should fail.
func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

// View behaves identically to Prepare except the result may not be
// committed back to the snapshot snapshotter. View returns a readonly view on
// the parent, with the active snapshot being tracked by the given key.
//
// This method operates identically to Prepare, except that Mounts returned
// may have the readonly flag set. Any modifications to the underlying
// filesystem will be ignored. Implementations may perform this in a more
// efficient manner that differs from what would be attempted with
// `Prepare`.
//
// Commit may not be called on the provided key and will return an error.
// To collect the resources associated with key, Remove must be called with
// key as the argument.
func (o *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

// Mounts returns the mounts for the active snapshot transaction identified
// by key. Can be called on an read-write or readonly transaction. This is
// available only for active snapshots.
//
// This can be used to recover mounts after calling View or Prepare.
func (o *snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}
	s, err := storage.GetSnapshot(ctx, key)
	t.Rollback()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot mount")
	}
	if s.Kind != snapshots.KindView && s.Kind != snapshots.KindActive {
		return nil, errors.Wrapf(err, "Mounts not allowed on %s snapshots", s.Kind)
	}
	return o.mounts(s), nil
}

// Commit captures the changes between key and its parent into a snapshot
// identified by name.  The name can then be used with the snapshotter's other
// methods to create subsequent snapshots.
//
// A committed snapshot will be created under name with the parent of the
// active snapshot.
//
// After commit, the snapshot identified by key is removed.
func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) (err error) {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil && t != nil {
			t.Rollback()
		}
	}()

	id, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return err
	}
	if info.Kind != snapshots.KindActive {
		return errors.Errorf("Commit called with %s snapshots", info.Kind)
	}

	source := o.getSnapshotPath(id, snapshots.KindActive)
	target := o.getSnapshotPath(id, snapshots.KindCommitted)

	usage, err := fs.DiskUsage(ctx, source)
	if err != nil {
		return err
	}

	if _, err := storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
		return errors.Wrap(err, "failed to commit snapshot")
	}

	if err := o.copyReflinkImage(source, target); err != nil {
		return err
	}
	defer func() {
		filename := source
		if err != nil {
			filename = target
		}
		if err1 := os.Remove(filename); err1 != nil {
			log.G(ctx).WithError(err1).Warnf("failed to remove snapshot file %s after commit", filename)
		}
	}()

	err = t.Commit()
	t = nil
	return err
}

// Remove the committed or active snapshot by the provided key.
//
// All resources associated with the key will be removed.
//
// If the snapshot is a parent of another snapshot, its children must be
// removed before proceeding.
func (o *snapshotter) Remove(ctx context.Context, key string) (err error) {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil && t != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	id, kind, err := storage.Remove(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to remove")
	}

	path := o.getSnapshotPath(id, kind)
	renamed := o.getSnapshotPath("rm-"+id, kind)
	if err := os.Rename(path, renamed); err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrap(err, "failed to rename")
		}
		renamed = ""
	}

	err = t.Commit()
	t = nil
	if err != nil {
		if renamed != "" {
			if err1 := os.Rename(renamed, path); err1 != nil {
				// May cause inconsistent data on disk
				log.G(ctx).WithError(err1).WithField("path", renamed).Errorf("failed to rename after failed commit")
			}
		}
		return errors.Wrap(err, "failed to commit")
	}
	if renamed != "" {
		if err := os.RemoveAll(renamed); err != nil {
			// Must be cleaned up, any "rm-*" could be removed if no active transactions
			log.G(ctx).WithError(err).WithField("path", renamed).Warnf("failed to remove snapshot file")
		}
	}

	return nil
}

// Walk all snapshots in the snapshotter. For each snapshot in the
// snapshotter, the function will be called.
func (o *snapshotter) Walk(ctx context.Context, fn func(context.Context, snapshots.Info) error) error {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}
	defer t.Rollback()
	return storage.WalkInfo(ctx, fn)
}

func (o *snapshotter) createBaseImage(ctx context.Context) (baseImage string, err error) {
	var base string
	if fd, err := ioutil.TempFile(filepath.Join(o.root, snapshots.KindActive.String()), "new-"); err != nil {
		return "", err
	} else {
		base = fd.Name()
		fd.Close()
	}
	defer func() {
		if err != nil {
			if err1 := os.Remove(base); err1 != nil {
				log.G(ctx).WithError(err1).Warnf("failed to remove temp file %s after base image creation failure", base)
			}
		}
	}()
	if err := os.Chmod(base, 0755); err != nil {
		return "", err
	}
	if err := os.Truncate(base, o.sizeMB<<20); err != nil {
		return "", err
	}
	switch o.fstype {
	case "xfs":
		if out, err := exec.Command("mkfs.xfs", "-f", base).CombinedOutput(); err != nil {
			return "", errors.Errorf("Failed to create %s file system on %s: %v:%s", o.fstype, base, err, string(out))
		}
	case "ext4":
		args := []string{
			base,
			"-F",
			"-O",
			"^uninit_bg",
			"-E",
			"nodiscard,lazy_itable_init=0,lazy_journal_init=0",
		}
		if out, err := exec.Command("mkfs.ext4", args...).CombinedOutput(); err != nil {
			return "", errors.Errorf("Failed to create %s file system on %s: %v:%s", o.fstype, base, err, string(out))
		}
		// Remove ext4's lost+found as we want an empty directory instead
		mount.WithTempMount(ctx, []mount.Mount{{Source: base, Type: o.fstype, Options: []string{"loop"}}},
			func(root string) error {
				return os.Remove(filepath.Join(root, "lost+found"))
			})
	default:
		return "", errors.Errorf("unsupported fstype %s", o.fstype)
	}

	return base, err
}

func (o *snapshotter) copyReflinkImage(source, target string) error {
	if out, err := exec.Command("cp", "-a", "--reflink=auto", source, target).CombinedOutput(); err != nil {
		return errors.Errorf("Failed to copy from %s to %s: %v:%s", source, target, err, string(out))
	}

	return nil
}

func (o *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (m []mount.Mount, err error) {
	var base string

	if len(parent) == 0 {
		base, err = o.createBaseImage(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create base image")
		}
		defer func() {
			if err != nil {
				if base != "" {
					if err1 := os.Remove(base); err1 != nil {
						err = errors.Wrapf(err, "remove failed: %v", err1)
					}
				}
			}
		}()
	}

	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil && t != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	// CreateSnapshot ensures that parent is committed if provided.
	s, err := storage.CreateSnapshot(ctx, kind, key, parent, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot")
	}

	current := o.getSnapshotPath(s.ID, kind)
	if len(s.ParentIDs) > 0 {
		parentPath := o.getSnapshotPath(s.ParentIDs[0], snapshots.KindCommitted)
		if err := o.copyReflinkImage(parentPath, current); err != nil {
			return nil, errors.Wrapf(err, "failed to copy image parent(%s):current(%s)", parentPath, current)
		}
	} else {
		if err := os.Rename(base, current); err != nil {
			return nil, errors.Wrap(err, "failed to rename")
		}
		base = ""
	}
	// Rollback current on failure
	defer func() {
		if err != nil {
			if err1 := os.Remove(current); err1 != nil {
				err = errors.Wrapf(err, "remove new snapshot file failed: %v", err1)
			}
		}
	}()

	// bergwolf: what if parent is removed before we do commit?
	err = t.Commit()
	t = nil
	if err != nil {
		return nil, errors.Wrap(err, "commit failed")
	}

	return o.mounts(s), nil
}

func (o *snapshotter) getSnapshotPath(id string, kind snapshots.Kind) string {
	return filepath.Join(o.root, kind.String(), id)
}

func (o *snapshotter) mounts(s storage.Snapshot) []mount.Mount {
	var (
		roFlag string
		source string
	)

	if s.Kind == snapshots.KindView || s.Kind == snapshots.KindCommitted {
		roFlag = "ro"
	} else {
		roFlag = "rw"
	}

	source = o.getSnapshotPath(s.ID, s.Kind)

	opts := []string{roFlag, "loop"}
	if len(o.options) != 0 {
		opts = append(opts, o.options)
	}
	// XFS needs nouuid or it can't mount filesystems with the same fs uuid
	if o.fstype == "xfs" {
		opts = append(opts, "nouuid")
	}

	return []mount.Mount{
		{
			Source:  source,
			Type:    o.fstype,
			Options: opts,
		},
	}
}

// Close releases the internal resources.
//
// Close is expected to be called on the end of the lifecycle of the snapshotter,
// but not mandatory.
//
// Close returns nil when it is already closed.
func (o *snapshotter) Close() error {
	return o.ms.Close()
}
