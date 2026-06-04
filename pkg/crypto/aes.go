package crypto // 声明当前文件属于crypto包，用于加解密操作

import (
	"crypto/aes"    // AES加密算法
	"crypto/cipher" // 加密模式
	"crypto/rand"   // 安全随机数
	"encoding/base64"
	"errors"
	"io"
	"strings"
)

// encPrefix 加密后密文的固定前缀，用于区分明文和密文
const encPrefix = "ENC~"

// Encrypt 使用AES-GCM加密明文，返回 base64 编码的密文（带前缀 ENC~）
// key 长度必须为 16/24/32 字节，分别对应 AES-128/192/256
func Encrypt(plaintext, key string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil) // nonce 前置到密文中
	return encPrefix + base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt 使用AES-GCM解密密文（需要带 ENC~ 前缀），返回明文
// 如果输入不带前缀，则视为明文直接返回（兼容旧数据）
func Decrypt(ciphertext, key string) (string, error) {
	if ciphertext == "" {
		return "", nil
	}
	if !strings.HasPrefix(ciphertext, encPrefix) {
		return ciphertext, nil // 兼容明文旧数据
	}
	data, err := base64.StdEncoding.DecodeString(ciphertext[len(encPrefix):])
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", errors.New("ciphertext too short")
	}
	nonce, raw := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, raw, nil)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

// IsEncrypted 判断字符串是否已经被加密（带 ENC~ 前缀）
func IsEncrypted(s string) bool {
	return strings.HasPrefix(s, encPrefix)
}

// NormalizeKey 将用户配置的密钥补齐或截断到 32 字节（AES-256）
// 密钥不足 32 字节时右侧补 0，超过 32 字节时截断
func NormalizeKey(key string) string {
	const keyLen = 32
	b := make([]byte, keyLen)
	copy(b, []byte(key))
	return string(b)
}
