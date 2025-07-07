# Your Vinaya Notes Module

**Fork this reprository to create your own Vinaya Notes Module!**

## Steps

1. Fork this repo
2. Fill in the metadata.yaml file with your folder's information
3. Update this README.md with your own introduction
4. Copy over your notes
5. Commit and push

## How it works

This repo comes with a GitHub Actions script (at .github/workflows/release.yaml)
which will automatically create a new GitHub Release with your Module every time
you push to the `main` branch.

Your latest .vnm file will always live at https://github.com/{{ your repo }}/releases/latest/download/manifest.vnm

## Customization

If you'd like to make a notes module with a custom build step, just modify the release.yaml file to add whatever build steps you need.
Just be sure to point the vinaya-notes-module-releaser step to the path of the folder you'd like to publish (and to the metadata yaml file if it's not in $path).

