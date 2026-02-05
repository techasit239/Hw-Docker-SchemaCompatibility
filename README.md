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

**Schema Evolution** โดยอ้างอิงจาก Logic ของระบบรถรับส่งพนักงาน (Employee Shuttle) ที่มีการปรับเปลี่ยนโครงสร้างข้อมูลจริงใน 3 เวอร์ชัน:
| Version | Changes (การเปลี่ยนแปลง) | Schema Definition (โครงสร้างข้อมูล) |
| :--- | :--- | :--- |
| **v1 (Base)** | - | `full_name`, `factory`, `position`, `dropoff_point` |
| **v2 (Add Field)** | **+Add:** `insurance`, `phone` | `full_name`, `factory`, `position`, `dropoff_point`, `insurance`*, `phone`*<br>*(Nullable & Default=null)* |
| **v3 (Remove Field)** | **-Remove:** `dropoff_point` | `full_name`, `factory`, `position`, `insurance`*, `phone`* |
<br>

## ผลการทดลอง (Experimental results)
### 6.1 โหมด Backward (Backward compatibility)

**Objective:** ตรวจสอบว่า **Consumer ตัวใหม่** สามารถอ่านข้อมูลที่ส่งมาด้วย **Schema ตัวเก่า** ได้หรือไม่
<br>
**Experiment details:**
| ID | Action & Scenario | Expected Result | Status | Note |
| :--- | :--- | :--- | :---: | :--- |
| **B-01** | **Action:** Add Optional Fields (v1 $\rightarrow$ v2)<br>**Scenario:** เพิ่ม `insurance`, `phone` (Default=null) | **Success**<br>(Auto-fill default) | ✅<br>PASS | Consumer v2 เติมค่า `null` ให้กับข้อมูล v1 ที่ไม่มีฟิลด์เหล่านี้ได้อัตโนมัติ |
| **B-02** | **Action:** Remove Field (v2 $\rightarrow$ v3)<br>**Scenario:** ลบ `dropoff_point` | **Success**<br>(Ignore field) | ✅<br>PASS | Consumer v3 มองข้ามฟิลด์ `dropoff_point` ที่ติดมากับข้อมูลเก่า (v2) ได้ |
| **B-03** | **Action:** Add Required Field (No Default)<br>**Scenario:** เพิ่มฟิลด์ `citizen_id` **โดยไม่ใส่ Default** | **Failure**<br>(Error 409) | ❌<br>FAIL | Consumer ใหม่พังทันที เพราะข้อมูลเก่าไม่มีค่านี้ส่งมาและไม่มี Default ให้ใช้ |
| **B-04** | **Action:** Change Type (Compatible)<br>**Scenario:** เปลี่ยน `int` เป็น `float` | **Success**<br>(Type Promotion) | ✅<br>PASS | **Updated:** Avro อนุญาตให้เปลี่ยน `int` (จำนวนเต็ม) เป็น `float` (ทศนิยม) ได้อย่างปลอดภัย (เช่น 5 $\rightarrow$ 5.0) |
| **B-05** | **Action:** Change Type (Incompatible)<br>**Scenario:** เปลี่ยน `int` เป็น `string` | **Failure**<br>(Type Mismatch) | ❌<br>FAIL | ไม่สามารถแปลงตัวเลขเป็นข้อความได้โดยตรง Consumer จะ Error |


### สรุปผลการทดลอง
เมื่อพยายามเพิ่มฟิลด์ใหม่ที่เป็น Required และไม่มีค่า Default (เช่น employee_id) ระบบ Schema Registry ปฏิเสธการลงทะเบียน Schema ด้วยข้อผิดพลาด
READER_FIELD_MISSING_DEFAULT_VALUE (HTTP 409)

<img width="809" height="278" alt="image" src="https://github.com/user-attachments/assets/5bf02240-b02a-4e27-9f6f-d9d5b7edf056" />

 
ใน Backward mode การเพิ่มฟิลด์ใหม่ต้องกำหนดให้เป็น Optional หรือมี Default value เสมอ เพื่อให้ Consumer เวอร์ชันใหม่สามารถอ่านข้อความย้อนหลังได้อย่างปลอดภัย

<img width="940" height="45" alt="image" src="https://github.com/user-attachments/assets/705e32c9-95ac-495f-94e2-2aba1730a21b" />
<img width="940" height="54" alt="image" src="https://github.com/user-attachments/assets/9647ef09-cacc-4885-9508-2a53398d8708" />

 
## 6.2 โหมด Forward (Forward compatibility)

**Objective:** ตรวจสอบว่า **Consumer ตัวเก่า** สามารถอ่านข้อมูลที่ส่งมาด้วย **Schema ตัวใหม่** ได้หรือไม่
<br>
**Experiment details:**
| ID | Action & Scenario | Expected Result | Status | Note |
| :--- | :--- | :--- | :---: | :--- |
| **F-01** | **Action:** Add Optional Fields (v1 $\rightarrow$ v2)<br>**Scenario:** ส่งข้อมูล v2 (มี `phone`) ให้ Consumer v1 | **Success**<br>(Ignore unknown) | ✅<br>PASS | Consumer v1 ไม่รู้จักฟิลด์ใหม่ จึงมองข้ามไปและอ่านข้อมูลส่วนที่เหลือได้ |
| **F-02** | **Action:** Remove Required Field (v2 $\rightarrow$ v3)<br>**Scenario:** ส่งข้อมูล v3 (ไม่มี `dropoff_point`) ให้ Consumer v2 | **Failure**<br>(Missing Required) | ❌<br>FAIL | **Breaking Change!** Consumer v2 จำเป็นต้องใช้ `dropoff_point` เมื่อ v3 ไม่ส่งมาให้ ระบบจึงล่ม |
| **F-03** | **Action:** Delete Field (with Default)<br>**Scenario:** ลบฟิลด์ที่มี Default ใน Schema เก่า | **Success**<br>(Use Local Default) | ✅<br>PASS | Consumer เก่าจะดึงค่า Default ในเครื่องตัวเองมาใช้แทนค่าที่หายไป |
| **F-04** | **Action:** Change Type (Risk)<br>**Scenario:** เปลี่ยน `float` เป็น `int` | **Failure**<br>(Precision Loss) | ❌<br>FAIL | **Updated:** การส่งข้อมูลทศนิยม (Float) ให้ Consumer ที่รอรับจำนวนเต็ม (Int) ทำไม่ได้ เพราะข้อมูลจะสูญหาย (เช่น 5.5 $\rightarrow$ 5) |
| **F-05** | **Action:** Change Type (Incompatible)<br>**Scenario:** เปลี่ยน `int` เป็น `string` | **Failure**<br>(Type Mismatch) | ❌<br>FAIL | Consumer เก่าคาดหวังตัวเลข แต่ได้รับข้อความ อ่านไม่ออกแน่นอน |


### สรุปผลการทดลอง
เมื่อ Producer ส่งข้อมูลด้วย Schema v2 และ Consumer ใช้ Schema v1 พบว่าสามารถอ่านข้อความได้ตามปกติ โดยฟิลด์ insurance และ phone ถูกละเว้นโดยอัตโนมัติ อย่างไรก็ตาม เมื่อทดลองเปลี่ยนชนิดข้อมูลของฟิลด์เดิม (factory จาก string เป็น int) ระบบปฏิเสธ Schema ด้วยข้อผิดพลาดTYPE_MISMATCH (HTTP 409)

<img width="940" height="49" alt="image" src="https://github.com/user-attachments/assets/572c4508-7e1e-4a4d-bbd2-6d857e0ad01a" />
<img width="940" height="67" alt="image" src="https://github.com/user-attachments/assets/e412badc-5149-4d48-86b3-f399238a7239" />

<img width="940" height="270" alt="image" src="https://github.com/user-attachments/assets/3986fdc8-4369-453c-9ac0-7c1b3bfd402a" />

 
Forward mode ไม่รองรับการเปลี่ยนชนิดข้อมูลของฟิลด์เดิม หากจำเป็นต้องเปลี่ยนควรใช้แนวทางเพิ่มฟิลด์ใหม่แทน เช่น factory_id และคงฟิลด์เดิมไว้ในช่วงเปลี่ยนผ่าน




## 6.3 โหมด Full (Full mode)

**Objective:** ตรวจสอบความเข้ากันได้ **ทั้งสองทิศทาง** (ปลอดภัยที่สุด upgradeฝั่งไหนก่อนก็ได้)
<br>
**Experiment details:**
| ID | Action & Scenario | Expected Result | Status | Note |
| :--- | :--- | :--- | :---: | :--- |
| **FULL-01** | **Action:** Add Optional Fields (v1 $\leftrightarrow$ v2)<br>**Scenario:** เพิ่ม `insurance`, `phone` (with default) | **Success**<br>(Bidirectional Safe) | ✅<br>PASS | ปลอดภัยทั้ง 2 ทาง: ขา Backward เติม Default, ขา Forward มองข้ามฟิลด์ |
| **FULL-02** | **Action:** Remove Required Field (v2 $\leftrightarrow$ v3)<br>**Scenario:** ลบ `dropoff_point` | **Failure**<br>(Fails Forward Check) | ❌<br>FAIL | พังที่ขา **Forward** (เหมือน case F-02) ทำให้ไม่ผ่านเกณฑ์ Full Mode |
| **FULL-03** | **Action:** Remove Optional Field<br>**Scenario:** ลบฟิลด์ `phone` (ที่มี Default null) | **Success**<br>(Bidirectional Safe) | ✅<br>PASS | หากลบฟิลด์ที่มี Default value จะถือว่าปลอดภัยทั้งสองทิศทาง |
| **FULL-04** | **Action:** Change Type<br>**Scenario:** สลับ `int` $\leftrightarrow$ `float` | **Failure**<br>(Strict Type Check) | ❌<br>FAIL | **Updated:** แม้ Backward จะผ่าน (Int $\rightarrow$ Float) แต่ Forward ไม่ผ่าน (Float $\rightarrow$ Int) จึงสรุปว่า **FAIL** |
| **FULL-05** | **Action:** Rename Field<br>**Scenario:** เปลี่ยนชื่อ `factory` เป็น `plant` | **Failure**<br>(Field Missing) | ❌<br>FAIL | Avro มองว่าคือการ "ลบ field เก่า" และ "เพิ่ม field ใหม่" พร้อมกัน ซึ่งมักจะติดเงื่อนไข Required Field |


### สรุปผลการทดลอง 
การเพิ่มฟิลด์ใหม่แบบ Optional ที่มี Default value ได้รับการยอมรับ อย่างไรก็ตาม การเปลี่ยนชนิดข้อมูลของฟิลด์เดิมถูกปฏิเสธโดย Schema Registry พร้อมข้อผิดพลาด TYPE_MISMATCH ในทั้งสองทิศทาง (reader และ writer)

<img width="940" height="286" alt="image" src="https://github.com/user-attachments/assets/11f13ff8-5551-45a7-a8ab-f2319c632b78" />


Full mode เป็นโหมดที่เข้มงวดที่สุด เหมาะสำหรับระบบ Production ที่ Producer และ Consumer อัปเกรดไม่พร้อมกัน โดยอนุญาตเฉพาะการเปลี่ยนแปลงที่ไม่ก่อให้เกิด Breaking changes

