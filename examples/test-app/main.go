package main

import (
	"fmt"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
)

func main() {
	fmt.Println("Hello, World!")
}

type TestAgg struct {
	core.Aggregate[TestAppState]
}

type TestAppState struct {
	Name string
}
