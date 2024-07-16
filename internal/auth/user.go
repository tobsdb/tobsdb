package auth

import (
	"errors"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

type TdbUserRole int

const (
	TdbUserRoleAdmin TdbUserRole = iota
	TdbUserRoleReadWrite
	TdbUserRoleReadOnly
	TdbUserRoleCheckSchema
)

var InsufficientPermissions = errors.New("Insufficient permissions")

func (u TdbUserRole) HasClearance(r TdbUserRole) bool { return u <= r }

type TdbUser struct {
	Id       string
	Name     string
	Password []byte
	IsRoot   bool
}

func NewUser(name, password string) *TdbUser {
	// password max size is 72 bytes because of bcrypt limit
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		panic(err)
	}
	return &TdbUser{uuid.New().String(), name, hashedPassword, false}
}

func (u *TdbUser) ValidateUser(password string) bool {
	return bcrypt.CompareHashAndPassword(u.Password, []byte(password)) == nil
}
