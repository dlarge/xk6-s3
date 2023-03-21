import s3 from 'k6/x/s3';

const client = s3.create(
  'ACCESS_KEY',
  'SECRET_KEY',
  'http://IP:PORT',
  'REGION');

export default function () {
  s3.upload(client, 'BUCKET_NAME', 'OBJECT_KEY', 'C:\\Path\\To\\File.bin');
}