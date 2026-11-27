// Command genicons draws the Storage Analyzer app icons: a blue rounded square
// with white usage bars, like an ncdu listing.
//
// Default (QPKG) mode writes 64px, 80px and gray 64px GIFs:
//
//	go run ./tools/genicons <outdir>
//
// fnOS mode writes 64px and 256px sRGB PNGs (ICON.PNG, ICON_256.PNG and
// app/ui/images/icon_{64,256}.png):
//
//	go run ./tools/genicons -fnos <outdir>
package main

import (
	"flag"
	"image"
	"image/color"
	"image/gif"
	"image/png"
	"log"
	"math"
	"os"
	"path/filepath"
)

const ss = 4 // supersampling factor

// coverage returns, for each output pixel, how much of it is covered by the
// background shape (bg) and by the white foreground (fg), both in [0,1].
// inset reserves a transparent margin around the rounded square (fraction of
// canvas width on each side, e.g. 0 for QPKG GIFs or 0.04 for fnOS PNGs).
func render(size int, inset float64) (bg, fg [][]float64) {
	n := size * ss
	pad := float64(n) * inset
	inner := float64(n) - 2*pad
	r := inner * 0.22 // corner radius
	inRound := func(x, y float64) bool {
		lx := x - pad
		ly := y - pad
		if lx < 0 || ly < 0 || lx > inner || ly > inner {
			return false
		}
		cx := math.Max(r, math.Min(inner-r, lx))
		cy := math.Max(r, math.Min(inner-r, ly))
		return (lx-cx)*(lx-cx)+(ly-cy)*(ly-cy) <= r*r
	}
	// bars: (y center, length fraction) in unit coordinates
	bars := []struct{ y, w float64 }{{0.30, 0.64}, {0.47, 0.44}, {0.64, 0.28}}
	barH := 0.105
	x0 := 0.20
	inBar := func(ux, uy float64) bool {
		for _, b := range bars {
			if uy >= b.y-barH/2 && uy <= b.y+barH/2 && ux >= x0 && ux <= x0+b.w {
				return true
			}
		}
		// baseline tick on the left
		return ux >= 0.14 && ux <= 0.165 && uy >= 0.22 && uy <= 0.80
	}
	bg = make([][]float64, size)
	fg = make([][]float64, size)
	for py := 0; py < size; py++ {
		bg[py] = make([]float64, size)
		fg[py] = make([]float64, size)
		for px := 0; px < size; px++ {
			var b, f float64
			for sy := 0; sy < ss; sy++ {
				for sx := 0; sx < ss; sx++ {
					x := float64(px*ss+sx) + 0.5
					y := float64(py*ss+sy) + 0.5
					if inRound(x, y) {
						b++
						if inBar((x-pad)/inner, (y-pad)/inner) {
							f++
						}
					}
				}
			}
			bg[py][px] = b / (ss * ss)
			fg[py][px] = f / (ss * ss)
		}
	}
	return bg, fg
}

func lerp(a, b color.RGBA, t float64) color.RGBA {
	m := func(x, y uint8) uint8 { return uint8(math.Round(float64(x) + (float64(y)-float64(x))*t)) }
	return color.RGBA{m(a.R, b.R), m(a.G, b.G), m(a.B, b.B), 255}
}

func drawGIF(size int, base color.RGBA) *image.Paletted {
	white := color.RGBA{255, 255, 255, 255}
	pal := color.Palette{color.RGBA{0, 0, 0, 0}}
	for i := 0; i < 255; i++ {
		pal = append(pal, lerp(base, white, float64(i)/254))
	}
	img := image.NewPaletted(image.Rect(0, 0, size, size), pal)
	bg, fg := render(size, 0)
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			if bg[y][x] < 0.5 {
				img.SetColorIndex(x, y, 0)
				continue
			}
			t := fg[y][x] / bg[y][x]
			img.SetColorIndex(x, y, uint8(1+math.Round(t*254)))
		}
	}
	return img
}

func drawPNG(size int, base color.RGBA) *image.NRGBA {
	white := color.RGBA{255, 255, 255, 255}
	img := image.NewNRGBA(image.Rect(0, 0, size, size))
	bg, fg := render(size, 0.04)
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			a := bg[y][x]
			if a <= 0 {
				continue
			}
			c := lerp(base, white, fg[y][x]/a)
			img.SetNRGBA(x, y, color.NRGBA{R: c.R, G: c.G, B: c.B, A: uint8(math.Round(a * 255))})
		}
	}
	return img
}

func writeGIF(path string, img *image.Paletted) {
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if err := gif.Encode(f, img, nil); err != nil {
		log.Fatal(err)
	}
}

func writePNG(path string, img *image.NRGBA) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if err := png.Encode(f, img); err != nil {
		log.Fatal(err)
	}
}

func main() {
	fnos := flag.Bool("fnos", false, "write fnOS PNG icons (ICON.PNG, ICON_256.PNG, app/ui/images/icon_{64,256}.png)")
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatal("usage: genicons [-fnos] <outdir>")
	}
	out := flag.Arg(0)
	if err := os.MkdirAll(out, 0o755); err != nil {
		log.Fatal(err)
	}
	blue := color.RGBA{0x1a, 0x6f, 0xd6, 255}
	gray := color.RGBA{0x9a, 0xa5, 0xb1, 255}
	if *fnos {
		i64 := drawPNG(64, blue)
		i256 := drawPNG(256, blue)
		writePNG(filepath.Join(out, "ICON.PNG"), i64)
		writePNG(filepath.Join(out, "ICON_256.PNG"), i256)
		writePNG(filepath.Join(out, "app", "ui", "images", "icon_64.png"), i64)
		writePNG(filepath.Join(out, "app", "ui", "images", "icon_256.png"), i256)
		return
	}
	writeGIF(filepath.Join(out, "StorageAnalyzer.gif"), drawGIF(64, blue))
	writeGIF(filepath.Join(out, "StorageAnalyzer_80.gif"), drawGIF(80, blue))
	writeGIF(filepath.Join(out, "StorageAnalyzer_gray.gif"), drawGIF(64, gray))
}
