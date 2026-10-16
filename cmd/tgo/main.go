// tgo builds and tests native Temporal workflows using the isolate toolchain.
package main

import (
	"github.com/mfateev/sdk-go-poc/internal/toolchain"
	"os"
)

func main() { os.Exit(toolchain.Main(os.Args[1:])) }
