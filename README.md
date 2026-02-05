# Hw-Docker-SchemaCompatibility
# การปรับเปลี่ยนโครงสร้างข้อมูล (Schema Evolution) สำหรับระบบคิวรถรับส่งพนักงาน กรณีศึกษาการเพิ่มข้อมูลประกันภัยและการติดต่อภายใต้โหมดความเข้ากันได้ของ Schema Registry

## ภาพรวม (Overview)
การศึกษานี้มุ่งสาธิตกระบวนการปรับเปลี่ยนโครงสร้างข้อมูล (Schema Evolution) ของข้อมูลพนักงานที่ใช้ในระบบบริหารจัดการคิวรถรับส่งพนักงาน โดยใช้ Apache Kafka ร่วมกับ Confluent Schema Registry เป็นแกนกลางของการจัดการความเข้ากันได้ของข้อมูล การทดลองเปรียบเทียบโหมดความเข้ากันได้แบบ Backward Forward และ Full เพื่อประเมินผลกระทบของการเปลี่ยนแปลงโครงสร้างข้อมูลต่อการทำงานร่วมกันระหว่างผู้ส่งข้อมูล (Producer) และผู้รับข้อมูล (Consumer) 
## รายละเอียดเฉพาะ (More specific details)
โครงสร้างข้อมูลเริ่มต้น (Schema เวอร์ชั่น 1 v1) ประกอบด้วยข้อมูลหลักที่จำเป็นต่อการจัดคิวและการวางแผนเส้นทาง ได้แก่ ชื่อ–นามสกุล โรงงานสังกัด ตำแหน่งงาน และจุดลงรถ ต่อมาได้มีการพัฒนา Schema เวอร์ชั่น 2 (v2) โดยเพิ่มข้อมูลด้านความปลอดภัย ได้แก่ ข้อมูลประกันภัยและหมายเลขโทรศัพท์ 
ซึ่งถูกออกแบบให้เป็นฟิลด์แบบ Optional และมีค่าเริ่มต้น (Default value) เพื่อรองรับการทำงานร่วมกับระบบเดิม
## ปัญหาที่มีอยู่เดิม (Existing issues)
ในการใช้งานระบบจริง ผู้ส่งข้อมูลและผู้รับข้อมูลมักได้รับการอัปเกรดระบบในช่วงเวลาที่แตกต่างกัน เนื่องจากแต่ละโรงงานสามารถดำเนินการเรื่องของประกันให้พนักงานไม่พร้อมกัน การเปลี่ยนแปลงโครงสร้างข้อมูลโดยไม่มีการกำหนดกฎความเข้ากันได้อย่างเหมาะสมอาจนำไปสู่ความล้มเหลวในการถอดรหัสข้อมูล (Deserialization failure) ซึ่งส่งผลกระทบต่อระบบรายงาน ระบบวิเคราะห์ข้อมูลปลายทาง และการประมวลผลข้อมูลแบบเรียลไทม์ โดยเฉพาะในระบบที่ต้องการความต่อเนื่องในการให้บริการ
## แรงจูงใจ (Motivation)
ระบบคิวรถรับส่งพนักงานจำเป็นต้องขยายขีดความสามารถเพื่อรองรับหลายโรงงานและมาตรการด้านความปลอดภัยที่เข้มงวดมากขึ้น ความถูกต้องและความต่อเนื่องของข้อมูล (Data reliability) 
มีความสำคัญอย่างยิ่งต่อการจัดการเหตุฉุกเฉินและการปฏิบัติตามกฎระเบียบ การเพิ่มข้อมูลประกันภัยและการติดต่อจึงเป็นสิ่งจำเป็น แต่ต้องดำเนินการโดยไม่ส่งผลกระทบต่อผู้ใช้งานข้อมูลเดิมหรือระบบที่ยังไม่
อัปเกรด

## โจทย์และวัตถุประสงค์ (Problem statement)
ข้อมูลนำเข้า (Input) Avro Schema จำนวน 2 เวอร์ชันสำหรับ Kafka Topic เดียวกัน ได้แก่
•	Employee Schema v1 ชื่อ–นามสกุล โรงงาน ตำแหน่ง จุดลงรถ
•	Employee Schema v2 เพิ่มข้อมูลประกันภัยและหมายเลขโทรศัพท์
## วัตถุประสงค์ (Objective) 
เพื่อประเมินพฤติกรรมของ Schema Registry ภายใต้โหมดความเข้ากันได้แบบ Backward, Forward และ Full โดยตรวจสอบว่า
1.	Consumer ที่ใช้ Schema ใหม่สามารถอ่านข้อความที่ถูกสร้างด้วย Schema เก่าได้หรือไม่ (Backward)
2.	Consumer ที่ใช้ Schema เก่าสามารถอ่านข้อความที่ถูกสร้างด้วย Schema ใหม่ได้หรือไม่ (Forward)
3.	การเปลี่ยนแปลงใดบ้างที่ถูกยอมรับหรือถูกปฏิเสธภายใต้โหมด Full

## ผลการทดลอง (Experimental results)
### 6.1 โหมด Backward (Backward compatibility)
**Objective:** Verify that the **new consumer** can read data produced with the **old schema**.

| ID | Action | Scenario Description | Expected Result | Status | Note |
| :----- | :--- | :--- | :--- | :---: | :--- |
| **B-01**   | Delete Field | Remove `ticket_total_value` field | **Success** | ✅ PASS | New consumer ignores the deleted field present in old data. |
| **B-02**   | Add Field (w/ Default) | Add `genre` field with default value | **Success** | ✅ PASS | New consumer fills in default value for missing field in old data. |
| **B-03**   | Add Field (No Default) | Add `director` field without default | **Failure** | ❌ FAIL | Error 409. New consumer cannot handle missing field without a default. |
| **B-04**   | Change Type (Compatible) | Change `int` to `long` | **Success** | ✅ PASS | Avro allows promotion from int to long. |
| **B-05**   | Change Type (Incompatible) | Change `int` to `string` | **Failure** | ❌ FAIL | Type mismatch. Cannot safely convert int to string in backward mode. |

### ผลการทดลอง
เมื่อพยายามเพิ่มฟิลด์ใหม่ที่เป็น Required และไม่มีค่า Default (เช่น employee_id) ระบบ Schema Registry ปฏิเสธการลงทะเบียน Schema ด้วยข้อผิดพลาด
READER_FIELD_MISSING_DEFAULT_VALUE (HTTP 409)

<img width="809" height="278" alt="image" src="https://github.com/user-attachments/assets/5bf02240-b02a-4e27-9f6f-d9d5b7edf056" />

 
ใน Backward mode การเพิ่มฟิลด์ใหม่ต้องกำหนดให้เป็น Optional หรือมี Default value เสมอ เพื่อให้ Consumer เวอร์ชันใหม่สามารถอ่านข้อความย้อนหลังได้อย่างปลอดภัย

<img width="940" height="45" alt="image" src="https://github.com/user-attachments/assets/705e32c9-95ac-495f-94e2-2aba1730a21b" />
<img width="940" height="54" alt="image" src="https://github.com/user-attachments/assets/9647ef09-cacc-4885-9508-2a53398d8708" />

 
## 6.2 โหมด Forward (Forward compatibility)
### พฤติกรรมที่คาดหวัง 
Consumer เวอร์ชันเก่าต้องสามารถอ่านข้อความที่ถูกสร้างด้วย Schema เวอร์ชันใหม่ได้ โดยจะละเลยฟิลด์ที่ไม่รู้จัก
### ผลการทดลอง
เมื่อ Producer ส่งข้อมูลด้วย Schema v2 และ Consumer ใช้ Schema v1 พบว่าสามารถอ่านข้อความได้ตามปกติ โดยฟิลด์ insurance และ phone ถูกละเว้นโดยอัตโนมัติ อย่างไรก็ตาม เมื่อทดลองเปลี่ยนชนิดข้อมูลของฟิลด์เดิม (factory จาก string เป็น int) ระบบปฏิเสธ Schema ด้วยข้อผิดพลาดTYPE_MISMATCH (HTTP 409)

<img width="940" height="49" alt="image" src="https://github.com/user-attachments/assets/572c4508-7e1e-4a4d-bbd2-6d857e0ad01a" />
<img width="940" height="67" alt="image" src="https://github.com/user-attachments/assets/e412badc-5149-4d48-86b3-f399238a7239" />

<img width="940" height="270" alt="image" src="https://github.com/user-attachments/assets/3986fdc8-4369-453c-9ac0-7c1b3bfd402a" />

 
Forward mode ไม่รองรับการเปลี่ยนชนิดข้อมูลของฟิลด์เดิม หากจำเป็นต้องเปลี่ยนควรใช้แนวทางเพิ่มฟิลด์ใหม่แทน เช่น factory_id และคงฟิลด์เดิมไว้ในช่วงเปลี่ยนผ่าน




## 6.3 โหมด Full (Full mode)
### พฤติกรรมที่คาดหวัง 
Schema ใหม่ต้องผ่านเงื่อนไขทั้ง Backward และ Forward พร้อมกัน
### ผลการทดลอง 
การเพิ่มฟิลด์ใหม่แบบ Optional ที่มี Default value ได้รับการยอมรับ อย่างไรก็ตาม การเปลี่ยนชนิดข้อมูลของฟิลด์เดิมถูกปฏิเสธโดย Schema Registry พร้อมข้อผิดพลาด TYPE_MISMATCH ในทั้งสองทิศทาง (reader และ writer)

<img width="940" height="286" alt="image" src="https://github.com/user-attachments/assets/11f13ff8-5551-45a7-a8ab-f2319c632b78" />


Full mode เป็นโหมดที่เข้มงวดที่สุด เหมาะสำหรับระบบ Production ที่ Producer และ Consumer อัปเกรดไม่พร้อมกัน โดยอนุญาตเฉพาะการเปลี่ยนแปลงที่ไม่ก่อให้เกิด Breaking changes

