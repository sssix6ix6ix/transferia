package util

import (
	"errors"
	"fmt"

	yslices "github.com/transferia/transferia/library/go/slices"
)

// ToString returns a string representation of errors. Any nil errors in the
// slice are skipped.
func ToString(errors []error) string {
	var out string
	for i, e := range errors {
		if e == nil {
			continue
		}
		if i != 0 {
			out += ", "
		}
		out += e.Error()
	}
	return out
}

// PrefixErrors prefixes each error within the supplied []error slice with the
// string pfx.
func PrefixErrors(errs []error, pfx string) []error {
	var nerr []error
	for _, err := range errs {
		nerr = append(nerr, fmt.Errorf("%s: %s", pfx, err))
	}
	return nerr
}

// MapErr is for applying mapping function to slice which may return error.
// All errors that occur during processing are stored in multi error object.
func MapErr[S ~[]T, T, M any](s S, fn func(T) (M, error)) ([]M, error) {
	var errs []error
	result := yslices.Map(s, func(subs T) M {
		subt, err := fn(subs)
		errs = append(errs, err)
		return subt
	})
	return result, errors.Join(errs...)
}

// ForEachErr is for calling function for each element of slice which may return error.
// All errors that occur during processing are stored in multi error object.
func ForEachErr[S ~[]T, T any](s S, fn func(T) error) error {
	var errs []error
	for _, t := range s {
		errs = append(errs, fn(t))
	}
	return errors.Join(errs...)
}
