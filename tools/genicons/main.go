// Command genicons draws the QPKG icons (64px, 80px and a gray 64px variant)
// as GIFs: a blue rounded square with white usage bars, like an ncdu listing.
//
//	go run ./tools/genicons <outdir>
package main

import (
	"image"
	"image/color"
	"image/gif"
	"log"
	"math"
	"os"
	"path/filepath"
)

const ss = 4 // supersampling factor

// coverage returns, for each output pixel, how much of it is covered by the
// background shape (bg) and by the white foreground (fg), both in [0,1].
func render(size int) (bg, fg [][]float64) {
	n := size * ss
	r := float64(n) * 0.22 // corner radius
	inRound := func(x, y float64) bool {
		cx := math.Max(r, math.Min(float64(n)-r, x))
		cy := math.Max(r, math.Min(float64(n)-r, y))
		return (x-cx)*(x-cx)+(y-cy)*(y-cy) <= r*r
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
						if inBar(x/float64(n), y/float64(n)) {
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

func draw(size int, base color.RGBA) *image.Paletted {
	white := color.RGBA{255, 255, 255, 255}
	pal := color.Palette{color.RGBA{0, 0, 0, 0}}
	for i := 0; i < 255; i++ {
		pal = append(pal, lerp(base, white, float64(i)/254))
	}
	img := image.NewPaletted(image.Rect(0, 0, size, size), pal)
	bg, fg := render(size)
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

func write(path string, img *image.Paletted) {
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if err := gif.Encode(f, img, nil); err != nil {
		log.Fatal(err)
	}
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: genicons <outdir>")
	}
	out := os.Args[1]
	if err := os.MkdirAll(out, 0o755); err != nil {
		log.Fatal(err)
	}
	blue := color.RGBA{0x1a, 0x6f, 0xd6, 255}
	gray := color.RGBA{0x9a, 0xa5, 0xb1, 255}
	write(filepath.Join(out, "StorageAnalyzer.gif"), draw(64, blue))
	write(filepath.Join(out, "StorageAnalyzer_80.gif"), draw(80, blue))
	write(filepath.Join(out, "StorageAnalyzer_gray.gif"), draw(64, gray))
}
