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
	"testing"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/pkg/testutil"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/testsuite"
	"github.com/containerd/continuity/testutil/loopback"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

const mkfsBin = "mkfs.btrfs"

const contentString = "Here_lieth_one_whos_name_was_writ_on_water"

func boltSnapshotter(t *testing.T) func(context.Context, string) (snapshots.Snapshotter, func() error, error) {
	mkfs, err := exec.LookPath(mkfsBin)
	if err != nil {
		t.Skipf("could not find %s: %v", mkfsBin, err)
	}

	return func(ctx context.Context, root string) (snapshots.Snapshotter, func() error, error) {

		loopbackSize := int64(512 << 20) // 512 MB
		deviceName, cleanupDevice, err := loopback.New(loopbackSize)

		if err != nil {
			return nil, nil, err
		}

		if out, err := exec.Command(mkfs, deviceName).CombinedOutput(); err != nil {
			cleanupDevice()
			return nil, nil, errors.Wrapf(err, "failed to make filesystem (out: %q)", out)
		}
		if out, err := exec.Command("mount", deviceName, root, "-o", "sync").CombinedOutput(); err != nil {
			cleanupDevice()
			return nil, nil, errors.Wrapf(err, "failed to mount device %s (out: %q)", deviceName, out)
		}

		snapshotter, err := NewSnapshotter(ctx, root)
		if err != nil {
			cleanupDevice()
			return nil, nil, errors.Wrap(err, "failed to create new snapshotter")
		}

		return snapshotter, func() error {
			if err := snapshotter.Close(); err != nil {
				return err
			}
			err := mount.UnmountAll(root, unix.MNT_DETACH)
			if cerr := cleanupDevice(); cerr != nil {
				err = errors.Wrap(cerr, "device cleanup failed")
			}
			return err
		}, nil
	}
}

func TestRawblock(t *testing.T) {
	testutil.RequiresRoot(t)
	testsuite.SnapshotterSuite(t, "Rawblock", boltSnapshotter(t))
}

func TestRawblockMount(t *testing.T) {
	testutil.RequiresRoot(t)
	ctx := context.Background()

	// create temporary directory for mount point
	mountPoint, err := ioutil.TempDir("", "containerd-rawblock-test")
	if err != nil {
		t.Fatal("could not create mount point for rawblock snapshotter test", err)
	}
	defer os.RemoveAll(mountPoint)

	root, err := ioutil.TempDir(mountPoint, "TestRawblockPrepare-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	b, c, err := boltSnapshotter(t)(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer c()

	key1 := "active1"
	target := filepath.Join(root, "test1")
	mounts, err := b.Prepare(ctx, key1, "")
	if err != nil {
		t.Fatal(err)
	}

	if len(mounts) != 1 {
		t.Fatalf("rawblock mounts MUST have only one layer: %+v", mounts)
	}

	if err := os.MkdirAll(target, 0755); err != nil {
		t.Fatal(err)
	}
	if err := mount.All(mounts, target); err != nil {
		t.Fatal(err)
	}

	// write in some data
	if err := ioutil.WriteFile(filepath.Join(target, "foo"), []byte(contentString), 0777); err != nil {
		t.Fatal(err)
	}

	testutil.Unmount(t, target)
	key2 := "committed1"
	if err := b.Commit(ctx, key2, key1); err != nil {
		t.Fatal(err)
	}

	target = filepath.Join(root, "test2")
	key3 := "active2"
	mounts, err = b.Prepare(ctx, key3, key2)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(target, 0755); err != nil {
		t.Fatal(err)
	}

	if err := mount.All(mounts, target); err != nil {
		t.Fatal(err)
	}

	// Verify "foo"
	if content, err := ioutil.ReadFile(filepath.Join(target, "foo")); err != nil {
		t.Fatal(err)
	} else if string(content) != contentString {
		t.Fatalf("foo contents do not match\n %s vs. %s\n", string(content), contentString)
	}

	if err := ioutil.WriteFile(filepath.Join(target, "bar"), []byte(contentString), 0777); err != nil {
		t.Fatal(err)
	}

	key4 := "committed2"
	testutil.Unmount(t, target)
	if err := b.Commit(ctx, key4, key3); err != nil {
		t.Fatal(err)
	}

	view := filepath.Join(root, "test3")
	key5 := "view"
	mounts, err = b.View(ctx, key5, key4)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(view, 0755); err != nil {
		t.Fatal(err)
	}

	if err := mount.All(mounts, view); err != nil {
		t.Fatal(err)
	}
	defer testutil.Unmount(t, view)

	// Verify "foo"
	if content, err := ioutil.ReadFile(filepath.Join(view, "foo")); err != nil {
		t.Fatal(err)
	} else if string(content) != contentString {
		t.Fatalf("foo contents do not match\n %s vs. %s\n", string(content), contentString)
	}

	// Verify "bar"
	if content, err := ioutil.ReadFile(filepath.Join(view, "foo")); err != nil {
		t.Fatal(err)
	} else if string(content) != contentString {
		t.Fatalf("bar contents do not match\n %s vs. %s\n", string(content), contentString)
	}

	// Write to view should fail
	if err := ioutil.WriteFile(filepath.Join(view, "bar"), []byte(contentString), 0777); err == nil {
		t.Fatal("write to view snapshot should fail")
	}

	if err := b.Commit(ctx, "foobar", key5); err == nil {
		t.Fatal("committing a view snapshot should fail")
	}

	if err := b.Remove(ctx, key5); err != nil {
		t.Fatal(err)
	}
}
