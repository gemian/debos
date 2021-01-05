/*
ImageFile Action

This action creates an image file

Yaml syntax:
 - action: image-file
   imagename: image_name
   imagesize: size
   fs: filesystem
   fsname: name

Mandatory properties:

 - imagename -- the name of the image file.

 - imagesize -- generated image size in human-readable form, examples: 100MB, 1GB, etc.

 - fs -- filesystem type used for formatting.

 - fsname -- filesystem name used in formatting.

Layout example:

 - action: image-file
   imagename: "debian.img"
   imagesize: 1GB
   fs: ext4
   fsname: debian
*/
package actions

import (
	"fmt"
	"github.com/docker/go-units"
	"github.com/go-debos/fakemachine"
	"gopkg.in/freddierice/go-losetup.v1"
	"log"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-debos/debos"
)

type ImageFileAction struct {
	debos.BaseAction `yaml:",inline"`
	ImageName        string
	ImageSize        string
	FS               string
	FSName           string
	size             int64
	loopDev          losetup.Device
	usingLoop        bool
}

func (i ImageFileAction) formatFile(context debos.DebosContext) error {
	label := fmt.Sprintf("Formatting file")

	cmdline := []string{}
	switch i.FS {
	case "vfat":
		cmdline = append(cmdline, "mkfs.vfat", "-F32", "-n", i.FSName)
	case "btrfs":
		// Force formatting to prevent failure in case if partition was formatted already
		cmdline = append(cmdline, "mkfs.btrfs", "-L", i.FSName, "-f")
	case "hfs":
		cmdline = append(cmdline, "mkfs.hfs", "-h", "-v", i.FSName)
	case "hfsplus":
		cmdline = append(cmdline, "mkfs.hfsplus", "-v", i.FSName)
	case "hfsx":
		cmdline = append(cmdline, "mkfs.hfsplus", "-s", "-v", i.FSName)
		// hfsx is case-insensitive hfs+, should be treated as "normal" hfs+ from now on
		i.FS = "hfsplus"
	case "none":
	default:
		cmdline = append(cmdline, fmt.Sprintf("mkfs.%s", i.FS), "-L", i.FSName)
	}

	if len(cmdline) != 0 {
		cmdline = append(cmdline, context.Image)

		cmd := debos.Command{}
		if err := cmd.Run(label, cmdline...); err != nil {
			return err
		}
	}

	return nil
}

func (i *ImageFileAction) PreNoMachine(context *debos.DebosContext) error {
	size, err := units.FromHumanSize(i.ImageSize)
	if err != nil {
		return fmt.Errorf("Failed to parse image size: %s", i.ImageSize)
	}
	i.size = size

	log.Printf("PreNoMachine: %s, size: %d",i.ImageName,i.size)

	img, err := os.OpenFile(i.ImageName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Couldn't open image file: %v", err)
	}

	err = img.Truncate(i.size)
	if err != nil {
		return fmt.Errorf("Couldn't resize image file: %v", err)
	}

	_ = img.Close()

	i.loopDev, err = losetup.Attach(i.ImageName, 0, false)
	if err != nil {
		return fmt.Errorf("Failed to setup loop device - need sudo?")
	}
	context.Image = i.loopDev.Path()
	i.usingLoop = true

	return nil
}

func (i ImageFileAction) PreMachine(context *debos.DebosContext, m *fakemachine.Machine, args *[]string) error {
	size, err := units.FromHumanSize(i.ImageSize)
	if err != nil {
		return fmt.Errorf("Failed to parse image size: %s", i.ImageSize)
	}
	i.size = size

	log.Printf("context.Rootdir: %s",context.Rootdir)
	log.Printf("PreMachine: %s, size: %d",i.ImageName,i.size)

	image, err := m.CreateImage(i.ImageName, i.size)
	if err != nil {
		return err
	}

	context.Image = image
	*args = append(*args, "--internal-image", image)
	return nil
}

func (i ImageFileAction) Run(context *debos.DebosContext) error {
	i.LogStart()
	log.Printf("Run: "+context.Image)
	/* Exclusively Lock image device file to prevent udev from triggering
	 * partition rescans, which cause confusion as some time asynchronously the
	 * partition device might disappear and reappear due to that! */
	imageFD, err := os.Open(context.Image)
	if err != nil {
		return err
	}
	/* Defer will keep the fd open until the function returns, at which points
	 * the filesystems will have been mounted protecting from more udev funnyness
	 */
	defer imageFD.Close()

	err = syscall.Flock(int(imageFD.Fd()), syscall.LOCK_EX)
	if err != nil {
		return err
	}

	err = i.formatFile(*context)
	if err != nil {
		return err
	}

	dev, _ := filepath.EvalSymlinks(context.Image)
	context.ImageMntDir = path.Join(context.Scratchdir, "mnt")
	_ = os.MkdirAll(context.ImageMntDir, 0755)
	err = syscall.Mount(dev, context.ImageMntDir, i.FS, 0, "")
	if err != nil {
		return fmt.Errorf("%s mount failed: %v", context.ImageMntDir, err)
	}

	return nil
}

func (i ImageFileAction) Cleanup(context *debos.DebosContext) error {
	err := syscall.Unmount(context.ImageMntDir, 0)
	if err != nil {
		log.Printf("Warning: Failed to get unmount %s: %s", context.ImageMntDir, err)
		log.Printf("Unmount failure can cause images being incomplete!")
		return err
	}

	if i.usingLoop {
		err := i.loopDev.Detach()
		if err != nil {
			log.Printf("WARNING: Failed to detach loop device: %s", err)
			return err
		}

		for t := 0; t < 60; t++ {
			err = i.loopDev.Remove()
			if err == nil {
				break
			}
			log.Printf("Loop dev couldn't remove %s, waiting", err)
			time.Sleep(time.Second)
		}

		if err != nil {
			log.Printf("WARNING: Failed to remove loop device: %s", err)
			return err
		}
	}

	//could also resize the image down to the minimum?
	//e2fsck -f debian.img
	//resize2fs -M debian.img

	return nil
}
