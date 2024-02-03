package conn

import "golang.org/x/crypto/bcrypt"

type TdbUserRole int

const (
	TdbUserRoleAdmin TdbUserRole = iota
	TdbUserRoleReadWrite
	TdbUserRoleReadOnly
	TdbUserRoleCheckSchema
)

type TdbUser struct {
	Id       int
	Name     string
	Password []byte
	Role     TdbUserRole
}

func NewUser(id int, name, password string, role TdbUserRole) *TdbUser {
	// password max size is 72 bytes because of bcrypt limit
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		panic(err)
	}
	return &TdbUser{id, name, hashedPassword, role}
}

func (u *TdbUser) ValidateUser(password string) bool {
	return bcrypt.CompareHashAndPassword(u.Password, []byte(password)) == nil
}

func (u *TdbUser) HasClearance(r TdbUserRole) bool { return u.Role <= r }
