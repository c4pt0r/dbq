package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	"dbq"
	"flag"

	"github.com/BurntSushi/toml"
	"github.com/bwmarrin/snowflake"
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

var defaultConfigToml string = `
[tidb]
username = "root"
password = ""
host = ""
port = 4000
database = "dbq"
tls_enabled = true

[tidb.tls_config]
server_name = ""

[server]
port = 8080
host = "localhost"
debug = true
`

type Config struct {
	TiDB   TiDBConfig   `toml:"tidb"`
	Server ServerConfig `toml:"server"`
}

type TiDBConfig struct {
	Username   string    `toml:"username"`
	Password   string    `toml:"password"`
	Host       string    `toml:"host"`
	Port       int       `toml:"port"`
	Database   string    `toml:"database"`
	TLSEnabled bool      `toml:"tls_enabled"`
	TLSConfig  TLSConfig `toml:"tls_config"`
}

type ServerConfig struct {
	Port  int    `toml:"port"`
	Host  string `toml:"host"`
	Debug bool   `toml:"debug"`
}

type TLSConfig struct {
	MinVersion string `toml:"min_version"`
	ServerName string `toml:"server_name"`
}

// define the API response format
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Payload interface{} `json:"payload"`
}

var (
	configFile         = flag.String("c", "", "config file")
	printDefaultConfig = flag.Bool("print-default-config", false, "print default config")
)

var (
	_nodeID int
	_node   *snowflake.Node

	_authToken string
)

func init() {
	var err error
	// read node id from env
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		_nodeID = 0
	} else {
		_nodeID, err = strconv.Atoi(nodeID)
		if err != nil {
			log.Fatal(err)
		}
	}

	_node, err = snowflake.NewNode(int64(_nodeID))
	if err != nil {
		log.Fatal(err)
	}

	_authToken = os.Getenv("AUTH_TOKEN")
}

func GenerateID() int64 {
	return _node.Generate().Int64()
}

func authMiddleware(c *gin.Context) {
	authHeader := c.GetHeader("Authorization")
	if authHeader == "" {
		c.JSON(http.StatusUnauthorized, APIResponse{
			Success: false,
			Message: "Authorization header is missing",
			Payload: nil,
		})
		c.Abort()
		return
	}

	token := authHeader[len("Bearer "):]
	if token != _authToken {
		c.JSON(http.StatusUnauthorized, APIResponse{
			Success: false,
			Message: "Invalid token",
			Payload: nil,
		})
		c.Abort()
		return
	}
	// Perform token validation and authentication logic here
	// For simplicity, let's assume token validation is successful
	c.Next()
}

// handle errors by returning an HTTP 500 error and the error message in the API response
func handleError(c *gin.Context, statusCode int, message string) {
	c.JSON(statusCode, APIResponse{
		Success: false,
		Message: message,
		Payload: nil,
	})
}

// create a new Q
func createQHandler(c *gin.Context) {
	name := c.Param("name")
	q := dbq.Q{}
	if _, err := q.New(name); err == nil {
		c.JSON(http.StatusOK, APIResponse{
			Success: true,
			Message: fmt.Sprintf("Q: %s created successfully", name),
			Payload: nil,
		})
	} else {
		handleError(c, http.StatusInternalServerError, err.Error())
	}
}

// delete a Q
func deleteQHandler(c *gin.Context) {
	name := c.Param("name")
	q := &dbq.Q{Name: name}
	if err := q.Drop(); err == nil {
		c.JSON(http.StatusOK, APIResponse{
			Success: true,
			Message: fmt.Sprintf("Q: %s deleted successfully", name),
			Payload: nil,
		})
	} else {
		handleError(c, http.StatusInternalServerError, err.Error())
	}
}

// push a new message to a Q
func pushMsgHandler(c *gin.Context) {
	name := c.Param("name")
	q := &dbq.Q{Name: name}
	// get data from body
	data, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		handleError(c, http.StatusBadRequest, "Invalid payload")
		return
	}
	// create a new message
	message := dbq.NewMsg(data)
	message.ID = GenerateID()
	if err := q.Push([]*dbq.Msg{message}); err == nil {
		c.JSON(http.StatusOK, APIResponse{
			Success: true,
			Payload: message.ID,
		})
	} else {
		handleError(c, http.StatusInternalServerError, err.Error())
	}
}

// pull messages from a Q
func pullMsgsHandler(c *gin.Context) {
	name := c.Param("name")
	q := &dbq.Q{Name: name}
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "100"))
	if err != nil {
		limit = 100
	}
	messages, err := q.Pull(limit)
	if err == nil {
		c.JSON(http.StatusOK, APIResponse{
			Success: true,
			Payload: messages,
		})
	} else {
		handleError(c, http.StatusInternalServerError, err.Error())
	}
}

// get a message by ID from a Q
func getMsgHandler(c *gin.Context) {
	name := c.Param("name")
	q := &dbq.Q{Name: name}
	id, _ := strconv.Atoi(c.Param("id"))
	message, err := q.GetMsgByID(int64(id))
	if err == nil {
		c.JSON(http.StatusOK, APIResponse{
			Success: true,
			Payload: message,
		})
	} else {
		handleError(c, http.StatusInternalServerError, err.Error())
	}
}

// update a message in a Q
func updateMsgHandler(c *gin.Context) {
	name := c.Param("name")
	q := &dbq.Q{Name: name}
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		handleError(c, http.StatusBadRequest, "Invalid message ID")
		return
	}
	// get message by id
	message := &dbq.Msg{ID: int64(id)}
	if err := c.ShouldBindJSON(message); err != nil {
		handleError(c, http.StatusBadRequest, fmt.Sprintf("Invalid payload: %s", err.Error()))
		return
	}

	if err := q.UpdateMsg(message); err == nil {
		c.JSON(http.StatusOK, APIResponse{
			Success: true,
			Payload: message.ID,
		})
	} else {
		handleError(c, http.StatusInternalServerError, err.Error())
	}
}

// clear a Q (delete all messages)
func clearQHandler(c *gin.Context) {
	name := c.Param("name")
	q := &dbq.Q{Name: name}
	if err := q.Clear(); err == nil {
		c.JSON(http.StatusOK, APIResponse{
			Success: true,
			Payload: nil,
		})
	} else {
		handleError(c, http.StatusInternalServerError, err.Error())
	}
}

func main() {
	flag.Parse()

	if *printDefaultConfig {
		fmt.Println(defaultConfigToml)
		return
	}

	if *configFile == "" {
		fmt.Println("Please specify a config file")
		return
	}

	var config Config
	if _, err := toml.DecodeFile(*configFile, &config); err != nil {
		log.Fatal(err)
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: config.TiDB.TLSConfig.ServerName,
	}

	if !config.TiDB.TLSEnabled {
		tlsConfig = nil
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=tidb&charset=utf8mb4&parseTime=True&loc=Local",
		config.TiDB.Username,
		config.TiDB.Password,
		config.TiDB.Host,
		config.TiDB.Port,
		config.TiDB.Database,
	)

	mysql.RegisterTLSConfig("tidb", tlsConfig)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	dbq.InitDB(db)

	r := gin.Default()
	// set middleware
	if len(_authToken) > 0 {
		log.Println("Auth token is set")
		r.Use(authMiddleware)
	}

	// define the API routes
	r.POST("/q/:name", createQHandler)
	r.DELETE("/q/:name", deleteQHandler)
	r.POST("/q/:name/push", pushMsgHandler)
	r.GET("/q/:name/pull", pullMsgsHandler)
	r.GET("/q/:name/msg/:id", getMsgHandler)
	r.PUT("/q/:name/msg/:id", updateMsgHandler)
	r.DELETE("/q/:name/truncate", clearQHandler)

	addr := fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port)
	log.Printf("Listening on %s...", addr)
	log.Fatal(r.Run(addr))
}
