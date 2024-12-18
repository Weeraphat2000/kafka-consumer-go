package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// createdCat คือฟังก์ชัน handler สำหรับข้อความที่ได้รับจาก topic "cat_created"
// รับ parameter เป็น kafka.Message จาก consumer และทำการประมวลผลข้อความ
func createdCat(msg kafka.Message) {
	fmt.Println("createdCat") // แสดงว่าฟังก์ชันนี้ถูกเรียกใช้งาน
	fmt.Printf("Handling cat_created: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
}

// updatedCat คือฟังก์ชัน handler สำหรับข้อความที่ได้รับจาก topic "cat_updated"
// รับ parameter เป็น kafka.Message จาก consumer และทำการประมวลผลข้อความ
func updatedCat(msg kafka.Message) {
	fmt.Println("updatedCat") // แสดงว่าฟังก์ชันนี้ถูกเรียกใช้งาน
	fmt.Printf("Handling cat_updated: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
}

// deletedCat คือฟังก์ชัน handler สำหรับข้อความที่ได้รับจาก topic "cat_deleted"
// รับ parameter เป็น kafka.Message จาก consumer และทำการประมวลผลข้อความ
func deletedCat(msg kafka.Message) {
	fmt.Println("deletedCat") // แสดงว่าฟังก์ชันนี้ถูกเรียกใช้งาน
	fmt.Printf("Handling cat_deleted: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
}

// consumeTopic เป็นฟังก์ชันสำหรับสร้างและรัน consumer สำหรับ topic ที่กำหนด
// parameter:
// - ctx: context เพื่อควบคุมการหยุดการทำงาน (เมื่อ ctx ถูก cancel จะหยุดการอ่านข้อความ)
// - broker: ที่อยู่ของ Kafka broker เช่น "localhost:9092"
// - topic: ชื่อ topic ที่ต้องการ consume
// - groupID: ชื่อกลุ่มของ consumer group
// - handler: ฟังก์ชันสำหรับประมวลผลข้อความแต่ละอันที่อ่านได้
func consumeTopic(ctx context.Context, broker string, topic string, groupID string, handler func(msg kafka.Message)) {
	// สร้าง Kafka Reader ด้วยการกำหนดค่า ReaderConfig
	// โดยระบุ broker, groupID, topic และการตั้งค่าการอ่าน message
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker}, // รายชื่อ broker
		GroupID:        groupID,          // ชื่อ consumer group
		Topic:          topic,            // ชื่อ topic ที่ต้องการอ่าน
		MinBytes:       10e3,             // จำนวน byte ขั้นต่ำในการอ่าน (10KB)
		MaxBytes:       10e6,             // จำนวน byte สูงสุดในการอ่าน (10MB)
		CommitInterval: time.Second,      // ระยะเวลา commit offset อัตโนมัติ
	})
	defer r.Close() // เมื่อฟังก์ชันจบ จะทำการปิด reader

	// วนลูปเพื่ออ่าน message จาก topic เรื่อย ๆ จนกว่าจะเกิด error หรือ context ถูก cancel
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			// ถ้าอ่าน message ไม่ได้ อาจเป็นเพราะถูก cancel หรือ broker มีปัญหา
			log.Printf("Failed to read message from topic %s: %v\n", topic, err)
			return
		}

		// หากอ่าน message ได้สำเร็จ จะเรียก handler function ที่ส่งเข้ามา
		handler(m)
	}
}

func main() {
	// สร้าง context และ cancel เพื่อให้สามารถหยุดการ consume ได้ภายหลังถ้าต้องการ
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ระบุ broker และ group ID สำหรับ consumer
	broker := "localhost:9092"
	groupID := "cat-consumer"

	// map สำหรับ mapping ระหว่างชื่อ topic และ handler function ที่ใช้ประมวลผล
	topicHandlers := map[string]func(msg kafka.Message){
		"cat_created": createdCat, // topic cat_created จะใช้ฟังก์ชัน createdCat ในการประมวลผล
		"cat_updated": updatedCat, // topic cat_updated จะใช้ฟังก์ชัน updatedCat ในการประมวลผล
		"cat_deleted": deletedCat, // topic cat_deleted จะใช้ฟังก์ชัน deletedCat ในการประมวลผล
	}

	// วน loop ใน map ของ topicHandlers เพื่อสร้าง goroutine consumer แต่ละ topic แยกกัน
	for topic, handler := range topicHandlers {
		go func(t string, h func(msg kafka.Message)) {
			// เรียก consumeTopic สำหรับ topic นั้น ๆ
			consumeTopic(ctx, broker, t, groupID, h)
		}(topic, handler)
	}

	// ปล่อยให้ main รันค้างไว้ 1 ชั่วโมง เพื่อทดสอบการ consume
	// ในกรณีจริงอาจจะรอ signal หรือ mechanism อื่น ๆ ในการ stop
	time.Sleep(time.Hour)
}
